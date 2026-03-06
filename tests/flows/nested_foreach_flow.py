"""Nested foreach flow: start(foreach) -> outer_step(foreach) -> inner_step -> inner_join -> outer_join -> end"""
from metaflow import FlowSpec, step


class NestedForeachFlow(FlowSpec):
    """A flow with a foreach inside a foreach (2 levels)."""

    @step
    def start(self):
        self.groups = ["a", "b"]
        self.next(self.outer_step, foreach="groups")

    @step
    def outer_step(self):
        self.items = [1, 2, 3]
        self.next(self.inner_step, foreach="items")

    @step
    def inner_step(self):
        self.result = self.input * 2
        self.next(self.inner_join)

    @step
    def inner_join(self, inputs):
        self.results = [i.result for i in inputs]
        self.next(self.outer_join)

    @step
    def outer_join(self, inputs):
        self.all_results = sorted([r for i in inputs for r in i.results])
        self.next(self.end)

    @step
    def end(self):
        assert sorted(self.all_results) == [2, 2, 4, 4, 6, 6], self.all_results
        print("NestedForeachFlow completed:", self.all_results)


if __name__ == "__main__":
    NestedForeachFlow()
