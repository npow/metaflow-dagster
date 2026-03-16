"""Foreach with two linear body steps before the join (multi-body foreach).

Topology: start(foreach) → process(linear) → transform(linear) → join
"""
from metaflow import FlowSpec, step


class MultibodyForeachFlow(FlowSpec):
    @step
    def start(self):
        self.items = [1, 2, 3]
        self.next(self.process, foreach="items")

    @step
    def process(self):
        self.processed = self.input * 2
        self.next(self.transform)

    @step
    def transform(self):
        self.result = self.processed + 1
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = sorted([i.result for i in inputs])
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    MultibodyForeachFlow()
