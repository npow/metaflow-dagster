"""Custom-named entry/terminal step flow using @step(start=True)/@step(end=True).

Regression fixture for Netflix/metaflow#3120 (start/end annotations) — verifies
that the dagster compiler does not hardcode the literal step name "start"/"end"
in generated job code.
"""
from metaflow import FlowSpec, step


class CustomNamedFlow(FlowSpec):
    @step(start=True)
    def entry(self):
        self.message = "begin"
        self.next(self.middle)

    @step
    def middle(self):
        self.message = self.message + " → middle"
        self.next(self.done)

    @step(end=True)
    def done(self):
        assert self.message == "begin → middle"


if __name__ == "__main__":
    CustomNamedFlow()
