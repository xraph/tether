# Results

One directory per run of the series in `../series.sh`. A directory is named for the date and the broker commit it measured. `RESULTS.md` reports the newest one. Each run writes `<name>-<n>.json` (the command's own report), `<name>-<n>.cell.json` (how many times a node fenced itself during the run) and, for storms, `<name>-<n>.storm.json` (the storm process's report).

- `2026-10-27-634de53/`: the runs `RESULTS.md` is built from. 48 runs on the broker at `634de53`, a quiet-machine guard of load average 10 (`MAX_LOAD`), 10,000 client storms.
- `2026-10-27-f25e789/`: a full second pass on the previous broker commit, including the `-rerun` series made under a load limit. The same scenarios, with some different load averages (4.8 to 14.5). It agrees with the newer pass and is kept as a second look.
- `2026-10-27-6dfe2dd/`: the first pass, on an older broker. It stamped each message with the moment the publisher woke for it, not the moment it was due, and ran its storms with 8,000 clients, so it is kept for comparison only. Don't quote it.
- `2026-10-27-6dfe2dd-discarded-storm/`: storm runs from the same pass whose storm process exited right after the storm, resetting 8,000 connections inside the measured phase. They overstate the storm's cost.
- `incident-port-exhaustion/`: log lines from an exploratory run on the older broker that asked for 45,000 clients. The load generator used up the machine's ephemeral ports, the NATS servers could no longer open routes, and the brokers fenced. It is a one-off and not a measurement.
