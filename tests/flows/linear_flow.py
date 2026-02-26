"""Simple linear flow: start → process → end"""
from metaflow import FlowSpec, step


class LinearFlow(FlowSpec):
    """A simple linear flow for testing the Dagster integration."""

    @step
    def start(self):
        self.message = "hello from start"
        self.next(self.process)

    @step
    def process(self):
        self.result = self.message + " -> process"
        self.next(self.end)

    @step
    def end(self):
        assert self.result == "hello from start -> process"
        print("LinearFlow completed:", self.result)


if __name__ == "__main__":
    LinearFlow()
