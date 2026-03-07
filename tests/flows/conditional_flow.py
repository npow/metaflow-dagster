"""Conditional (split-switch) flow: start (split-switch) → high_branch/low_branch → join → end"""
from metaflow import FlowSpec, Parameter, step


class ConditionalFlow(FlowSpec):
    """A conditional flow that takes different branches based on a parameter."""

    value = Parameter("value", default=42, type=int)

    @step
    def start(self):
        self.route = "high" if self.value >= 50 else "low"
        self.next({"high": self.high_branch, "low": self.low_branch}, condition="route")

    @step
    def high_branch(self):
        self.result = "HIGH: %d" % self.value
        self.next(self.join)

    @step
    def low_branch(self):
        self.result = "LOW: %d" % self.value
        self.next(self.join)

    @step
    def join(self):
        self.final = self.result
        self.next(self.end)

    @step
    def end(self):
        print("Result:", self.final)


if __name__ == "__main__":
    ConditionalFlow()
