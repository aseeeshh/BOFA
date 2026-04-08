import argparse
import sys
from itertools import product

import pandas as pd
from tabulate import tabulate


BONDS_FILE  = "bonds.xlsx"
EVENTS_FILE = "events.xlsx"

# Maps user-facing dimension names to DataFrame column names
DIM_TO_COL = {"bond": "BondID", "trader": "Trader", "desk": "Desk"}
COL_TO_DIM = {v: k for k, v in DIM_TO_COL.items()}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data():
    bonds  = pd.read_excel(BONDS_FILE,  engine="openpyxl", header=1)
    events = pd.read_excel(EVENTS_FILE, engine="openpyxl")
    events = events.sort_values("EventID").reset_index(drop=True)
    return bonds, events


# ---------------------------------------------------------------------------
# Calculations
# ---------------------------------------------------------------------------

def compute_accrued_interest(coupon, frequency, months_since_coupon):
    period_length    = 12 / frequency
    accrued_fraction = months_since_coupon / period_length
    return (coupon / frequency) * accrued_fraction * 100


def process_events(events: pd.DataFrame, bonds: pd.DataFrame) -> pd.DataFrame:
    bond_accrued = {
        row["BondID"]: compute_accrued_interest(
            row["Coupon"], row["Frequency"], row["MonthsSinceCoupon"]
        )
        for _, row in bonds.iterrows()
    }

    position = {}   # BondID -> running net quantity
    last_pv  = {}   # BondID -> PV after most recent event

    rows = []
    for _, ev in events.iterrows():
        bond_id   = ev["BondID"]
        qty       = ev["Quantity"]
        buy_sell  = ev["BuySell"].strip().upper()
        accrued   = bond_accrued.get(bond_id, 0.0)
        dirty     = ev["CleanPrice"] + accrued

        prev_pos = position.get(bond_id, 0)
        if buy_sell == "BUY":
            new_pos = prev_pos + qty
        elif buy_sell == "SELL":
            new_pos = prev_pos - qty
        else:
            new_pos = prev_pos

        position[bond_id] = new_pos
        new_pv  = new_pos * dirty
        delta   = new_pv - last_pv.get(bond_id, 0.0)
        last_pv[bond_id] = new_pv

        rows.append({
            "EventID":         ev["EventID"],
            "Desk":            ev["Desk"],
            "Trader":          ev["Trader"],
            "BondID":          bond_id,
            "BuySell":         buy_sell,
            "Quantity":        qty,
            "CleanPrice":      ev["CleanPrice"],
            "AccruedInterest": accrued,
            "DirtyPrice":      dirty,
            "Position":        new_pos,
            "PV":              new_pv,
            "DeltaPV":         delta,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def filter_events(df, from_event, to_event):
    if from_event is not None:
        df = df[df["EventID"] >= from_event]
    if to_event is not None:
        df = df[df["EventID"] <= to_event]
    return df


def filter_entities(df, entity_filters):
    """
    entity_filters: dict of col -> list of allowed values, e.g.
        {"BondID": ["BOND1", "BOND3"], "Trader": ["T_NY_1"]}
    Only applied for dimensions that have specific (non-All) selections.
    """
    for col, values in entity_filters.items():
        if values:                          # empty list means "All" — skip
            df = df[df[col].isin(values)]
    return df


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate(df: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    """
    Aggregate by one or more columns (e.g. ["BondID"], ["Trader"], ["BondID","Trader"]).

    When BondID is among the group columns, AccruedInterest and DirtyPrice
    are included (they are bond-level constants / last-seen price).
    When BondID is not among the group columns, positions and PV are
    summed across all bonds within each group.
    """
    has_bond = "BondID" in group_cols

    if has_bond:
        # Each group uniquely identifies a bond — take last event per group
        last = df.sort_values("EventID").groupby(group_cols).last().reset_index()
        delta = df.groupby(group_cols)["DeltaPV"].sum().reset_index(name="TotalDeltaPV")
        result = last.merge(delta, on=group_cols)

        keep = group_cols + ["Position", "AccruedInterest", "DirtyPrice", "PV", "TotalDeltaPV"]
        result = result[keep].copy()
        result = result.rename(columns={"PV": "PresentValue"})

    else:
        # Groups span multiple bonds — sum across bonds within each group
        inner_keys = group_cols + ["BondID"]
        last  = df.sort_values("EventID").groupby(inner_keys).last().reset_index()
        delta = df.groupby(inner_keys)["DeltaPV"].sum().reset_index(name="TotalDeltaPV")
        merged = last.merge(delta, on=inner_keys)

        result = (
            merged.groupby(group_cols)
            .agg(Position=("Position", "sum"),
                 PresentValue=("PV", "sum"),
                 TotalDeltaPV=("TotalDeltaPV", "sum"))
            .reset_index()
        )

    return result.sort_values(group_cols).reset_index(drop=True)


def fill_missing_entities(result: pd.DataFrame, group_cols: list,
                          universe: dict) -> pd.DataFrame:
    """
    Add dash rows for entity combinations that were in the universe
    (i.e. user asked to see them) but had no trades in the filtered range.

    universe: dict of col -> sorted list of all expected values.
    """
    if not universe:
        return result

    # Build the full expected index as a cross-product of universe values
    keys   = list(universe.keys())
    combos = list(product(*[universe[k] for k in keys]))
    full   = pd.DataFrame(combos, columns=keys)

    merged = full.merge(result, on=keys, how="left")

    # Fill numeric NaNs with "-" (convert whole df to object first)
    numeric_cols = result.select_dtypes(include="number").columns.tolist()
    for col in numeric_cols:
        merged[col] = merged[col].apply(lambda v: v if pd.notna(v) else "-")

    return merged.sort_values(group_cols).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def fmt(value, decimals=2):
    if isinstance(value, float):
        return f"{value:,.{decimals}f}"
    return value


def display(df: pd.DataFrame, group_cols: list, from_event, to_event,
            entity_labels: dict = None):
    dim_names  = " + ".join(COL_TO_DIM.get(c, c).upper() for c in group_cols)
    event_range = f"{from_event or 'start'} to {to_event or 'end'}"

    # Entity filter summary line
    filter_parts = []
    if entity_labels:
        for col, vals in entity_labels.items():
            if vals:
                filter_parts.append(f"{COL_TO_DIM.get(col, col).upper()}: {', '.join(vals)}")
    filter_line = ("  Filters: " + "  |  ".join(filter_parts)) if filter_parts else ""

    W = max(62, len(filter_line) + 2)
    print(f"\n{'=' * W}")
    print(f"  Bond Aggregation Engine  |  Grouped by: {dim_names}")
    print(f"  Event range: {event_range}")
    if filter_line:
        print(filter_line)
    print(f"{'=' * W}\n")

    NUMERIC_COLS = {"Position", "AccruedInterest", "DirtyPrice", "PresentValue", "TotalDeltaPV"}
    display_df = df.copy()
    for col in display_df.columns:
        if col not in NUMERIC_COLS:
            continue
        decimals = 4 if col == "AccruedInterest" else 2
        display_df[col] = display_df[col].apply(
            lambda v: fmt(v, decimals) if isinstance(v, (int, float)) else v
        )

    print(tabulate(display_df, headers="keys", tablefmt="simple", showindex=False))
    print()


# ---------------------------------------------------------------------------
# Interactive menu helpers
# ---------------------------------------------------------------------------

MENU_WIDTH = 62

def _hr(char="="):
    print(char * MENU_WIDTH)

def _header(title):
    _hr()
    print(f"  {title}")
    _hr()

def _prompt(msg, default=None):
    suffix = f" [{default}]" if default is not None else ""
    return input(f"  {msg}{suffix}: ").strip()

def _choose(prompt, options, labels=None):
    if labels is None:
        labels = [str(o) for o in options]
    print()
    for i, label in enumerate(labels, 1):
        print(f"  [{i}] {label}")
    print()
    while True:
        raw = _prompt(prompt)
        if raw == "":
            return options[0]
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1]
        print(f"  Please enter a number between 1 and {len(options)}.")

def _choose_multi(prompt, options, labels=None):
    """Return a list of chosen option values (comma-separated input)."""
    if labels is None:
        labels = [str(o) for o in options]
    print()
    for i, label in enumerate(labels, 1):
        print(f"  [{i}] {label}")
    print()
    while True:
        raw = _prompt(f"{prompt} (comma-separated, e.g. 1,3)")
        if raw == "":
            return [options[0]]
        parts = [p.strip() for p in raw.split(",")]
        if all(p.isdigit() and 1 <= int(p) <= len(options) for p in parts):
            return [options[int(p) - 1] for p in parts]
        print(f"  Please enter numbers between 1 and {len(options)}, comma-separated.")

def _select_entities(dim_label, all_values):
    """
    Ask whether to filter a dimension to specific values.
    Returns a list of selected values, or [] meaning "All".
    """
    print()
    print(f"  Filter {dim_label} to specific values?")
    choice = _choose(
        "Select option",
        options=["all", "specific"],
        labels=[f"All {dim_label}s", f"Choose specific {dim_label}s"],
    )
    if choice == "all":
        return []

    print()
    print(f"  Select {dim_label}s to include:")
    chosen = _choose_multi(
        f"Select {dim_label}s",
        options=all_values,
        labels=all_values,
    )
    return chosen

def _ask_event_range(min_id, max_id):
    print()
    raw_from = _prompt(f"From EventID (press Enter for {min_id})", default=min_id)
    raw_to   = _prompt(f"To   EventID (press Enter for {max_id})", default=max_id)
    try:
        from_event = int(raw_from) if raw_from else min_id
    except ValueError:
        print("  Invalid — using start of range.")
        from_event = min_id
    try:
        to_event = int(raw_to) if raw_to else max_id
    except ValueError:
        print("  Invalid — using end of range.")
        to_event = max_id
    if from_event > to_event:
        print("  From > To — swapping.")
        from_event, to_event = to_event, from_event
    return from_event, to_event


# ---------------------------------------------------------------------------
# Interactive menu (main loop)
# ---------------------------------------------------------------------------

def interactive_menu(processed, bonds_df, min_id, max_id):
    # Pre-build universe of each dimension from the full dataset
    all_values = {
        "BondID": sorted(bonds_df["BondID"].unique().tolist()),
        "Trader": sorted(processed["Trader"].unique().tolist()),
        "Desk":   sorted(processed["Desk"].unique().tolist()),
    }

    while True:
        print()
        _header("Bond Aggregation Engine")
        print()
        print("  What would you like to do?")
        action = _choose("Select option",
                         options=["run", "exit"],
                         labels=["Run a report", "Exit"])
        if action == "exit":
            print("\n  Goodbye.\n")
            sys.exit(0)

        # --- Choose grouping dimensions (multi-select) ---
        print()
        print("  Group results by (you can combine dimensions):")
        dims = _choose_multi(
            "Select grouping",
            options=["bond", "trader", "desk"],
            labels=[
                "Bond   — one row per bond",
                "Trader — aggregated per trader",
                "Desk   — aggregated per desk (NY / HK / LN)",
            ],
        )
        group_cols = [DIM_TO_COL[d] for d in dims]

        # --- Entity filter for each chosen dimension ---
        entity_filters = {}
        for col in group_cols:
            dim_label = COL_TO_DIM[col].capitalize()
            selected = _select_entities(dim_label, all_values[col])
            entity_filters[col] = selected   # [] = All

        # --- Event range ---
        print()
        print("  Set event range:")
        range_choice = _choose(
            "Select range option",
            options=["all", "custom"],
            labels=[
                f"All events  (EventID {min_id} – {max_id})",
                "Custom range",
            ],
        )
        if range_choice == "all":
            from_event, to_event = None, None
        else:
            from_event, to_event = _ask_event_range(min_id, max_id)

        # --- Process ---
        filtered = filter_events(processed, from_event, to_event)
        filtered = filter_entities(filtered, entity_filters)

        if filtered.empty:
            print("\n  No events match the selected filters. Try different options.\n")
            continue

        result = aggregate(filtered, group_cols)

        # Build universe for dash-filling: only dims with "All" selected
        # (specific selections already limit the data; show all of the universe
        #  that the user asked for, filling dashes where there's no activity)
        universe = {}
        for col in group_cols:
            if entity_filters[col]:
                # User picked specific values — use those as the universe
                universe[col] = entity_filters[col]
            else:
                # User said "All" — use the full known set
                universe[col] = all_values[col]

        result = fill_missing_entities(result, group_cols, universe)

        # Only show entity filter labels that have specific values
        active_filters = {col: vals for col, vals in entity_filters.items() if vals}
        display(result, group_cols, from_event, to_event, active_filters)

        # --- Post-result ---
        print("  What next?")
        next_action = _choose("Select option",
                              options=["again", "exit"],
                              labels=["Run another report", "Exit"])
        if next_action == "exit":
            print("\n  Goodbye.\n")
            sys.exit(0)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Bond Aggregation Engine — process trading events and compute bond metrics."
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
    parser.add_argument("--to-event",   type=int, default=None, metavar="N")
    parser.add_argument("--filter-bond",   nargs="+", default=[], metavar="ID")
    parser.add_argument("--filter-trader", nargs="+", default=[], metavar="ID")
    parser.add_argument("--filter-desk",   nargs="+", default=[], metavar="ID")
    args = parser.parse_args()

    bonds, events = load_data()
    processed = process_events(events, bonds)
    min_id = int(processed["EventID"].min())
    max_id = int(processed["EventID"].max())

    non_interactive = (
        args.aggregate_by is not None
        or args.from_event is not None
        or args.to_event   is not None
        or args.filter_bond
        or args.filter_trader
        or args.filter_desk
    )

    if non_interactive:
        dims       = args.aggregate_by or ["bond"]
        group_cols = [DIM_TO_COL[d] for d in dims]
        entity_filters = {
            "BondID": args.filter_bond,
            "Trader": args.filter_trader,
            "Desk":   args.filter_desk,
        }

        filtered = filter_events(processed, args.from_event, args.to_event)
        filtered = filter_entities(filtered, entity_filters)

        if filtered.empty:
            print("No events match the selected filters.")
            sys.exit(1)

        result = aggregate(filtered, group_cols)

        all_values = {
            "BondID": sorted(bonds["BondID"].unique().tolist()),
            "Trader": sorted(processed["Trader"].unique().tolist()),
            "Desk":   sorted(processed["Desk"].unique().tolist()),
        }
        universe = {}
        for col in group_cols:
            sel = entity_filters.get(col, [])
            universe[col] = sel if sel else all_values[col]

        result = fill_missing_entities(result, group_cols, universe)
        active_filters = {col: vals for col, vals in entity_filters.items() if vals}
        display(result, group_cols, args.from_event, args.to_event, active_filters)

    else:
        interactive_menu(processed, bonds, min_id, max_id)


if __name__ == "__main__":
    main()
