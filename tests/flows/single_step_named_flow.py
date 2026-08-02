"""Single-step flow with a custom name (Netflix/metaflow#3120).

Regression fixture: Metaflow upgrades the entry step type to "end" (not
"start") for single-step flows, because the same step is both start and end.
The dagster compiler must route this through _render_start_op without
raising NotImplementedError("start step with type 'end'").
"""
from metaflow import FlowSpec, step


class SingleStepNamedFlow(FlowSpec):
    @step(start=True, end=True)
    def only(self):
        self.message = "single-step done"


if __name__ == "__main__":
    SingleStepNamedFlow()
