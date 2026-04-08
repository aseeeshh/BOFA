from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from itertools import product
from pathlib import Path
from typing import Any

import pandas as pd
from tabulate import tabulate


BASE_DIR = Path(__file__).resolve().parent
BONDS_FILE = BASE_DIR / "bonds.xlsx"
EVENTS_FILE = BASE_DIR / "events.xlsx"

DIM_TO_COL = {"bond": "BondID", "trader": "Trader", "desk": "Desk"}
COL_TO_DIM = {value: key for key, value in DIM_TO_COL.items()}
NUMERIC_REPORT_COLUMNS = {
    "Position",
    "AccruedInterest",
    "DirtyPrice",
    "PresentValue",
    "TotalDeltaPV",
}


@dataclass(frozen=True)
class Bond:
    bond_id: str
    coupon: float
    frequency: int
    months_since_coupon: float

    @classmethod
    def from_record(cls, row: pd.Series) -> "Bond":
        return cls(
            bond_id=str(row["BondID"]),
            coupon=float(row["Coupon"]),
            frequency=int(row["Frequency"]),
            months_since_coupon=float(row["MonthsSinceCoupon"]),
        )

    def accrued_interest(self) -> float:
        if self.frequency <= 0:
            raise ValueError(f"Bond {self.bond_id} has invalid coupon frequency: {self.frequency}")
        period_length = 12 / self.frequency
        accrued_fraction = self.months_since_coupon / period_length
        return (self.coupon / self.frequency) * accrued_fraction * 100


@dataclass(frozen=True)
class Event:
    event_id: int
    desk: str
    trader: str
    bond_id: str
    buy_sell: str
    quantity: float
    clean_price: float

    @classmethod
    def from_record(cls, row: pd.Series) -> "Event":
        return cls(
            event_id=int(row["EventID"]),
            desk=str(row["Desk"]),
            trader=str(row["Trader"]),
            bond_id=str(row["BondID"]),
            buy_sell=str(row["BuySell"]),
            quantity=float(row["Quantity"]),
            clean_price=float(row["CleanPrice"]),
        )

    @property
    def side(self) -> str:
        return self.buy_sell.strip().upper()

    def position_change(self) -> float:
        if self.side == "BUY":
            return self.quantity
        if self.side == "SELL":
            return -self.quantity
        return 0.0


@dataclass(frozen=True)
class ProcessedEvent:
    event: Event
    accrued_interest: float
    dirty_price: float
    position: float
    present_value: float
    delta_present_value: float

    def to_record(self) -> dict[str, Any]:
        return {
            "EventID": self.event.event_id,
            "Desk": self.event.desk,
            "Trader": self.event.trader,
            "BondID": self.event.bond_id,
            "BuySell": self.event.side,
            "Quantity": self.event.quantity,
            "CleanPrice": self.event.clean_price,
            "AccruedInterest": self.accrued_interest,
            "DirtyPrice": self.dirty_price,
            "Position": self.position,
            "PV": self.present_value,
            "DeltaPV": self.delta_present_value,
        }


@dataclass(frozen=True)
class ReportRequest:
    group_dimensions: list[str]
    from_event: int | None = None
    to_event: int | None = None
    entity_filters: dict[str, list[str]] = field(default_factory=dict)

    @property
    def group_columns(self) -> list[str]:
        if not self.group_dimensions:
            raise ValueError("At least one grouping dimension is required.")
        return [DIM_TO_COL[dimension] for dimension in self.group_dimensions]

    @property
    def active_filters(self) -> dict[str, list[str]]:
        return {column: values for column, values in self.entity_filters.items() if values}


class ExcelRepository:
    def __init__(self, bonds_path: Path = BONDS_FILE, events_path: Path = EVENTS_FILE) -> None:
        self.bonds_path = bonds_path
        self.events_path = events_path

    def load_bonds(self) -> dict[str, Bond]:
        frame = pd.read_excel(self.bonds_path, engine="openpyxl", header=1)
        bonds = [Bond.from_record(row) for _, row in frame.iterrows()]
        return {bond.bond_id: bond for bond in bonds}

    def load_events(self) -> list[Event]:
        frame = pd.read_excel(self.events_path, engine="openpyxl")
        events = [Event.from_record(row) for _, row in frame.iterrows()]
        return sorted(events, key=lambda event: event.event_id)

    def load(self) -> tuple[dict[str, Bond], list[Event]]:
        return self.load_bonds(), self.load_events()


class BondAggregationEngine:
    def __init__(self, bonds: dict[str, Bond], events: list[Event]) -> None:
        self.bonds = bonds
        self.events = sorted(events, key=lambda event: event.event_id)
        self.processed_events = self._process_events()
        self.processed_frame = pd.DataFrame(
            [processed_event.to_record() for processed_event in self.processed_events]
        )
        self.all_values = {
            "BondID": sorted(self.bonds.keys()),
            "Trader": sorted({event.trader for event in self.events}),
            "Desk": sorted({event.desk for event in self.events}),
        }

    def _process_events(self) -> list[ProcessedEvent]:
        positions: dict[str, float] = {}
        last_present_values: dict[str, float] = {}
        processed_events: list[ProcessedEvent] = []

        for event in self.events:
            bond = self.bonds.get(event.bond_id)
            accrued_interest = bond.accrued_interest() if bond else 0.0
            dirty_price = event.clean_price + accrued_interest

            new_position = positions.get(event.bond_id, 0.0) + event.position_change()
            positions[event.bond_id] = new_position

            present_value = new_position * dirty_price
            delta_present_value = present_value - last_present_values.get(event.bond_id, 0.0)
            last_present_values[event.bond_id] = present_value

            processed_events.append(
                ProcessedEvent(
                    event=event,
                    accrued_interest=accrued_interest,
                    dirty_price=dirty_price,
                    position=new_position,
                    present_value=present_value,
                    delta_present_value=delta_present_value,
                )
            )

        return processed_events

    def event_id_range(self) -> tuple[int, int]:
        if self.processed_frame.empty:
            raise ValueError("No processed events are available.")
        return (
            int(self.processed_frame["EventID"].min()),
            int(self.processed_frame["EventID"].max()),
        )

    def build_report(self, request: ReportRequest) -> pd.DataFrame:
        filtered = self._filter_by_event_range(
            self.processed_frame,
            request.from_event,
            request.to_event,
        )
        filtered = self._filter_by_entities(filtered, request.entity_filters)

        if filtered.empty:
            return filtered

        result = self._aggregate(filtered, request.group_columns)
        universe = self._build_universe(request)
        return self._fill_missing_entities(result, request.group_columns, universe)

    def _build_universe(self, request: ReportRequest) -> dict[str, list[str]]:
        universe: dict[str, list[str]] = {}
        for column in request.group_columns:
            selected_values = request.entity_filters.get(column, [])
            universe[column] = selected_values if selected_values else self.all_values[column]
        return universe

    @staticmethod
    def _filter_by_event_range(
        frame: pd.DataFrame,
        from_event: int | None,
        to_event: int | None,
    ) -> pd.DataFrame:
        filtered = frame
        if from_event is not None:
            filtered = filtered[filtered["EventID"] >= from_event]
        if to_event is not None:
            filtered = filtered[filtered["EventID"] <= to_event]
        return filtered

    @staticmethod
    def _filter_by_entities(
        frame: pd.DataFrame,
        entity_filters: dict[str, list[str]],
    ) -> pd.DataFrame:
        filtered = frame
        for column, values in entity_filters.items():
            if values:
                filtered = filtered[filtered[column].isin(values)]
        return filtered

    @staticmethod
    def _aggregate(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
        has_bond = "BondID" in group_columns

        if has_bond:
            last = frame.sort_values("EventID").groupby(group_columns).last().reset_index()
            delta = frame.groupby(group_columns)["DeltaPV"].sum().reset_index(name="TotalDeltaPV")
            result = last.merge(delta, on=group_columns)

            keep = group_columns + [
                "Position",
                "AccruedInterest",
                "DirtyPrice",
                "PV",
                "TotalDeltaPV",
            ]
            result = result[keep].copy()
            result = result.rename(columns={"PV": "PresentValue"})
        else:
            inner_keys = group_columns + ["BondID"]
            last = frame.sort_values("EventID").groupby(inner_keys).last().reset_index()
            delta = frame.groupby(inner_keys)["DeltaPV"].sum().reset_index(name="TotalDeltaPV")
            merged = last.merge(delta, on=inner_keys)

            result = (
                merged.groupby(group_columns)
                .agg(
                    Position=("Position", "sum"),
                    PresentValue=("PV", "sum"),
                    TotalDeltaPV=("TotalDeltaPV", "sum"),
                )
                .reset_index()
            )

        return result.sort_values(group_columns).reset_index(drop=True)

    @staticmethod
    def _fill_missing_entities(
        result: pd.DataFrame,
        group_columns: list[str],
        universe: dict[str, list[str]],
    ) -> pd.DataFrame:
        if not universe:
            return result

        keys = list(universe.keys())
        combinations = list(product(*[universe[key] for key in keys]))
        full_index = pd.DataFrame(combinations, columns=keys)
        merged = full_index.merge(result, on=keys, how="left")

        numeric_columns = result.select_dtypes(include="number").columns.tolist()
        for column in numeric_columns:
            merged[column] = merged[column].apply(lambda value: value if pd.notna(value) else "-")

        return merged.sort_values(group_columns).reset_index(drop=True)


class ReportRenderer:
    def display(self, frame: pd.DataFrame, request: ReportRequest) -> None:
        dim_names = " + ".join(COL_TO_DIM.get(column, column).upper() for column in request.group_columns)
        event_range = f"{request.from_event or 'start'} to {request.to_event or 'end'}"

        filter_parts = []
        for column, values in request.active_filters.items():
            filter_parts.append(f"{COL_TO_DIM.get(column, column).upper()}: {', '.join(values)}")
        filter_line = ("  Filters: " + "  |  ".join(filter_parts)) if filter_parts else ""

        width = max(62, len(filter_line) + 2)
        print(f"\n{'=' * width}")
        print(f"  Bond Aggregation Engine  |  Grouped by: {dim_names}")
        print(f"  Event range: {event_range}")
        if filter_line:
            print(filter_line)
        print(f"{'=' * width}\n")

        display_frame = frame.copy()
        for column in display_frame.columns:
            if column not in NUMERIC_REPORT_COLUMNS:
                continue
            decimals = 4 if column == "AccruedInterest" else 2
            display_frame[column] = display_frame[column].apply(
                lambda value: self._format_value(value, decimals)
            )

        print(tabulate(display_frame, headers="keys", tablefmt="simple", showindex=False))
        print()

    @staticmethod
    def _format_value(value: Any, decimals: int = 2) -> Any:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return f"{value:,.{decimals}f}"
        return value


class ConsoleMenu:
    def __init__(
        self,
        engine: BondAggregationEngine,
        renderer: ReportRenderer,
        menu_width: int = 62,
    ) -> None:
        self.engine = engine
        self.renderer = renderer
        self.menu_width = menu_width

    def run(self) -> None:
        min_id, max_id = self.engine.event_id_range()

        while True:
            print()
            self._header("Bond Aggregation Engine")
            print()
            print("  What would you like to do?")
            action = self._choose(
                "Select option",
                options=["run", "exit"],
                labels=["Run a report", "Exit"],
            )
            if action == "exit":
                print("\n  Goodbye.\n")
                raise SystemExit(0)

            request = self._build_request(min_id, max_id)
            report = self.engine.build_report(request)

            if report.empty:
                print("\n  No events match the selected filters. Try different options.\n")
                continue

            self.renderer.display(report, request)

            print("  What next?")
            next_action = self._choose(
                "Select option",
                options=["again", "exit"],
                labels=["Run another report", "Exit"],
            )
            if next_action == "exit":
                print("\n  Goodbye.\n")
                raise SystemExit(0)

    def _build_request(self, min_id: int, max_id: int) -> ReportRequest:
        print()
        print("  Group results by (you can combine dimensions):")
        dimensions = self._choose_multi(
            "Select grouping",
            options=["bond", "trader", "desk"],
            labels=[
                "Bond   - one row per bond",
                "Trader - aggregated per trader",
                "Desk   - aggregated per desk (NY / HK / LN)",
            ],
        )

        group_columns = [DIM_TO_COL[dimension] for dimension in dimensions]
        entity_filters: dict[str, list[str]] = {}
        for column in group_columns:
            dimension_label = COL_TO_DIM[column].capitalize()
            entity_filters[column] = self._select_entities(
                dimension_label,
                self.engine.all_values[column],
            )

        print()
        print("  Set event range:")
        range_choice = self._choose(
            "Select range option",
            options=["all", "custom"],
            labels=[
                f"All events  (EventID {min_id} - {max_id})",
                "Custom range",
            ],
        )
        if range_choice == "all":
            from_event, to_event = None, None
        else:
            from_event, to_event = self._ask_event_range(min_id, max_id)

        return ReportRequest(
            group_dimensions=dimensions,
            from_event=from_event,
            to_event=to_event,
            entity_filters=entity_filters,
        )

    def _hr(self, char: str = "=") -> None:
        print(char * self.menu_width)

    def _header(self, title: str) -> None:
        self._hr()
        print(f"  {title}")
        self._hr()

    @staticmethod
    def _prompt(message: str, default: Any = None) -> str:
        suffix = f" [{default}]" if default is not None else ""
        return input(f"  {message}{suffix}: ").strip()

    def _choose(
        self,
        prompt: str,
        options: list[str],
        labels: list[str] | None = None,
    ) -> str:
        option_labels = labels or [str(option) for option in options]
        print()
        for index, label in enumerate(option_labels, start=1):
            print(f"  [{index}] {label}")
        print()

        while True:
            raw = self._prompt(prompt)
            if raw == "":
                return options[0]
            if raw.isdigit() and 1 <= int(raw) <= len(options):
                return options[int(raw) - 1]
            print(f"  Please enter a number between 1 and {len(options)}.")

    def _choose_multi(
        self,
        prompt: str,
        options: list[str],
        labels: list[str] | None = None,
    ) -> list[str]:
        option_labels = labels or [str(option) for option in options]
        print()
        for index, label in enumerate(option_labels, start=1):
            print(f"  [{index}] {label}")
        print()

        while True:
            raw = self._prompt(f"{prompt} (comma-separated, e.g. 1,3)")
            if raw == "":
                return [options[0]]
            parts = [part.strip() for part in raw.split(",")]
            if all(part.isdigit() and 1 <= int(part) <= len(options) for part in parts):
                return [options[int(part) - 1] for part in parts]
            print(f"  Please enter numbers between 1 and {len(options)}, comma-separated.")

    def _select_entities(self, dimension_label: str, all_values: list[str]) -> list[str]:
        print()
        print(f"  Filter {dimension_label} to specific values?")
        choice = self._choose(
            "Select option",
            options=["all", "specific"],
            labels=[f"All {dimension_label}s", f"Choose specific {dimension_label}s"],
        )
        if choice == "all":
            return []

        print()
        print(f"  Select {dimension_label}s to include:")
        return self._choose_multi(
            f"Select {dimension_label}s",
            options=all_values,
            labels=all_values,
        )

    def _ask_event_range(self, min_id: int, max_id: int) -> tuple[int, int]:
        print()
        raw_from = self._prompt(f"From EventID (press Enter for {min_id})", default=min_id)
        raw_to = self._prompt(f"To   EventID (press Enter for {max_id})", default=max_id)

        try:
            from_event = int(raw_from) if raw_from else min_id
        except ValueError:
            print("  Invalid - using start of range.")
            from_event = min_id

        try:
            to_event = int(raw_to) if raw_to else max_id
        except ValueError:
            print("  Invalid - using end of range.")
            to_event = max_id

        if from_event > to_event:
            print("  From > To - swapping.")
            from_event, to_event = to_event, from_event

        return from_event, to_event


class BondAggregationApp:
    def __init__(self, engine: BondAggregationEngine, renderer: ReportRenderer) -> None:
        self.engine = engine
        self.renderer = renderer

    @staticmethod
    def build_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Bond Aggregation Engine - process trading events and compute bond metrics."
        )
        parser.add_argument(
            "--aggregate-by",
            nargs="+",
            choices=["bond", "trader", "desk"],
            default=None,
            metavar="DIM",
            help="One or more dimensions: bond trader desk",
        )
        parser.add_argument("--from-event", type=int, default=None, metavar="N")
        parser.add_argument("--to-event", type=int, default=None, metavar="N")
        parser.add_argument("--filter-bond", nargs="+", default=[], metavar="ID")
        parser.add_argument("--filter-trader", nargs="+", default=[], metavar="ID")
        parser.add_argument("--filter-desk", nargs="+", default=[], metavar="ID")
        return parser

    @staticmethod
    def is_non_interactive(args: argparse.Namespace) -> bool:
        return any(
            [
                args.aggregate_by is not None,
                args.from_event is not None,
                args.to_event is not None,
                bool(args.filter_bond),
                bool(args.filter_trader),
                bool(args.filter_desk),
            ]
        )

    def run(self, args: argparse.Namespace) -> None:
        if self.is_non_interactive(args):
            self._run_non_interactive(args)
            return

        ConsoleMenu(self.engine, self.renderer).run()

    def _run_non_interactive(self, args: argparse.Namespace) -> None:
        request = ReportRequest(
            group_dimensions=args.aggregate_by or ["bond"],
            from_event=args.from_event,
            to_event=args.to_event,
            entity_filters={
                "BondID": args.filter_bond,
                "Trader": args.filter_trader,
                "Desk": args.filter_desk,
            },
        )

        report = self.engine.build_report(request)
        if report.empty:
            print("No events match the selected filters.")
            raise SystemExit(1)

        self.renderer.display(report, request)


def main() -> None:
    args = BondAggregationApp.build_parser().parse_args()
    bonds, events = ExcelRepository().load()
    engine = BondAggregationEngine(bonds, events)
    renderer = ReportRenderer()
    app = BondAggregationApp(engine, renderer)
    app.run(args)


if __name__ == "__main__":
    main()
