"""Foreach flow: start → process_item (×N) → join → end"""
from metaflow import FlowSpec, step


class ForeachFlow(FlowSpec):
    """A foreach flow that maps over a list of items."""

    @step
    def start(self):
        self.items = ["apple", "banana", "cherry"]
        self.next(self.process_item, foreach="items")

    @step
    def process_item(self):
        self.processed = self.input.upper()
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = [i.processed for i in inputs]
        self.next(self.end)

    @step
    def end(self):
        assert sorted(self.results) == ["APPLE", "BANANA", "CHERRY"]
        print("ForeachFlow completed:", self.results)


if __name__ == "__main__":
    ForeachFlow()
