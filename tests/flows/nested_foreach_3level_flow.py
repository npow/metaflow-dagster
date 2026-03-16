"""3-level nested foreach flow — exercises the compiler's deep compound-op path.

Topology: start → outer(foreach) → middle(foreach) → inner(foreach)
                → inner_join → middle_join → outer_join → end

This exposes the compiler bug where _generate_job_body looks up inner foreach
step names in the chains dict, which is keyed only by outermost foreach names.
"""

from metaflow import FlowSpec, step


class NestedForeach3levelFlow(FlowSpec):
    @step
    def start(self):
        self.groups = ["a", "b"]
        self.next(self.outer, foreach="groups")

    @step
    def outer(self):
        self.group = self.input
        self.batches = [1, 2]
        self.next(self.middle, foreach="batches")

    @step
    def middle(self):
        self.batch = self.input
        self.items = [10, 20]
        self.next(self.inner, foreach="items")

    @step
    def inner(self):
        self.result = f"{self.group}-{self.batch}-{self.input}"
        self.next(self.inner_join)

    @step
    def inner_join(self, inputs):
        self.batch_results = sorted(i.result for i in inputs)
        self.next(self.middle_join)

    @step
    def middle_join(self, inputs):
        self.group_results = sorted(r for i in inputs for r in i.batch_results)
        self.next(self.outer_join)

    @step
    def outer_join(self, inputs):
        self.all_results = sorted(r for i in inputs for r in i.group_results)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    NestedForeach3LevelFlow()
