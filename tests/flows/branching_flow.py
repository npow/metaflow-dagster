"""Branch/join flow: start → (branch_a, branch_b) → join → end"""
from metaflow import FlowSpec, step


class BranchingFlow(FlowSpec):
    """A branching flow that splits and rejoins."""

    @step
    def start(self):
        self.value = 10
        self.next(self.branch_a, self.branch_b)

    @step
    def branch_a(self):
        self.a_result = self.value * 2
        self.next(self.join)

    @step
    def branch_b(self):
        self.b_result = self.value + 5
        self.next(self.join)

    @step
    def join(self, inputs):
        self.merged_a = inputs.branch_a.a_result
        self.merged_b = inputs.branch_b.b_result
        self.next(self.end)

    @step
    def end(self):
        assert self.merged_a == 20
        assert self.merged_b == 15
        print("BranchingFlow completed: a=%d b=%d" % (self.merged_a, self.merged_b))


if __name__ == "__main__":
    BranchingFlow()
