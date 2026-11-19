# Lab 10: Write the Dashboard Design Documents

## Goal

Write two connected engineering design documents for the Personal Investment
Dashboard:

1. A high-level design of the whole system.
2. A medium-level design of one feature that you choose.

Use the method in
[Engineering Design Documents, RFCs, and ADRs](../../lectures/lecture-10-engineering-design-documents-rfcs-and-adrs.md).
Prepare a design that another engineer can review and implement. Application
code is not required.

## Starting point

Use your reviewed Dashboard work from Labs 1-5:

- Market overview, trending Stocks, filtering, and search.
- Stock detail, delayed prices, and daily price history.
- Private Watchlist reads, additions, and removals.
- Market-data collection, accepted stored data, and freshness rules.
- Scaled application instances, read replicas, local caches, and Redis.
- The logical data model and HTTP API from Lab 5.

## 1. Whole-system design document

Write `SYSTEM-DESIGN.md` at a high level. Include:

1. **Problem and scope.** State the product promise, actors, goals, non-goals,

   and main constraints.
2. **Functional requirements.** Group observable behavior under `MUST`,

   `SHOULD`, and `MUST NOT` as needed.
3. **Quality and workload.** Select the important existing targets. State the

   measure, target, and operating condition. Identify assumptions.
4. **Architecture.** Draw a System View and an overview Container View. Show

   the system boundary, external dependencies, applications, stores, and their
    responsibilities. Summarize which records settle important state.
5. **Main flow and failures.** Show one User journey at the system boundary.

   State its success point and the User-visible result of a dependency failure.
6. **Solutions and decision.** Compare two viable whole-system arrangements

   against the same requirements. Select one and state the deciding reason  
    and one negative consequence.
7. **Risks and open questions.** Give each important item an owner and next

   action. State when an unanswered question must be resolved.

## 2. Feature design document

Select one feature within the Dashboard scope. Examples:

- Market-data collection with validation, saved sync progress, and restart recovery.
- Stock price reads through local memory, Redis, and stored accepted prices.
- Trending, filter, or search results with pagination, shared caching, and failures.
- Private Watchlist changes with ownership checks, safe repeats, and lost responses.

You can choose another feature within the same product scope. State why it
needs a medium-level design. It must include:

- interactions between applications, stores, or an external dependency;
- important stored data and at least one correctness or access rule;
- a normal path and a meaningful failure and recovery path;
- two viable ways to meet the same feature requirements.

A screen layout, a field rename, or one simple CRUD endpoint is too small.  
Bound the feature so that you can explain it completely.
