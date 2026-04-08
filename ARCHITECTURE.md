# Bond Aggregation Engine Architecture

## Overview

The project now follows a class-based design where each part of the system has one clear job:

- `Bond` models a bond and knows how to compute its own accrued interest.
- `Event` models a trading event and knows how that event changes position.
- `ProcessedEvent` represents the result of applying one event to the running portfolio state.
- `ExcelRepository` loads bonds and events from Excel and converts raw rows into domain objects.
- `BondAggregationEngine` contains the business logic for processing events, filtering data, aggregating reports, and filling missing combinations.
- `ReportRenderer` handles console formatting and display only.
- `ConsoleMenu` handles the interactive user experience only.
- `BondAggregationApp` coordinates CLI mode versus interactive mode.

This is a better OOP structure because the code is no longer one long script where loading, calculations, reporting, and user interaction are mixed together.

## How The Data Flows

1. `ExcelRepository` reads `bonds.xlsx` and `events.xlsx`.
2. Each bond row becomes a `Bond` object.
3. Each event row becomes an `Event` object.
4. `BondAggregationEngine` walks through events in `EventID` order.
5. For each event:
   - the matching `Bond` provides accrued interest,
   - the `Event` provides the signed position change,
   - the engine updates running position and present value for that bond,
   - the engine creates a `ProcessedEvent`.
6. All processed events are converted into a pandas DataFrame.
7. When a report is requested, the engine:
   - filters by event range,
   - filters by selected entities,
   - aggregates by the requested dimensions,
   - fills in missing bond/trader/desk combinations with `-`.
8. `ReportRenderer` prints the final table.

## Class Responsibilities

### `Bond`

`Bond` is the domain model for a security. It stores:

- `bond_id`
- `coupon`
- `frequency`
- `months_since_coupon`

Its key behavior is `accrued_interest()`. That keeps bond-specific pricing logic inside the bond itself instead of scattering the formula across the application.

### `Event`

`Event` is the domain model for one trade event. It stores:

- `event_id`
- `desk`
- `trader`
- `bond_id`
- `buy_sell`
- `quantity`
- `clean_price`

Its key behavior is `position_change()`. A `BUY` adds quantity and a `SELL` subtracts quantity. That means the engine does not need to know how to interpret every event string in multiple places.

### `ProcessedEvent`

`ProcessedEvent` is the ledger-style result after the engine applies one event. It stores:

- the original `Event`
- accrued interest
- dirty price
- new running position
- present value
- delta present value

This gives you a clean separation between raw input data and calculated output data.

### `ExcelRepository`

This class isolates the storage layer. The rest of the system does not care that the source is Excel. If you later want CSV, SQL, or an API, you can replace this class without rewriting the engine.

### `BondAggregationEngine`

This is the business logic layer. It is responsible for:

- processing events into portfolio state,
- filtering by event IDs and entity selections,
- aggregating report results,
- building the universe of expected rows,
- filling missing report combinations.

This is the heart of the application. It uses `Bond` and `Event` objects, but it does not handle user prompts or output formatting.

### `ReportRenderer`

This class only knows how to print a report. It does not load files, process trades, or choose filters. That separation makes presentation changes much easier later.

### `ConsoleMenu`

This class only manages the interactive menu flow:

- choose grouping dimensions,
- choose entity filters,
- choose event ranges,
- request another report or exit.

It delegates all calculations to the engine and all output formatting to the renderer.

### `BondAggregationApp`

This is the top-level coordinator. It decides whether to:

- run in CLI mode with arguments, or
- launch the interactive menu.
