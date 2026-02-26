"""Parametrized flow with Metaflow Parameters."""
from metaflow import FlowSpec, Parameter, step


class ParametrizedFlow(FlowSpec):
    """A flow that uses Metaflow Parameters."""

    greeting = Parameter("greeting", default="Hello", help="Greeting prefix")
    count = Parameter("count", default=3, type=int, help="Number of repetitions")

    @step
    def start(self):
        self.messages = [f"{self.greeting} #{i}" for i in range(self.count)]
        self.next(self.end)

    @step
    def end(self):
        assert len(self.messages) == self.count
        print("ParametrizedFlow completed:", self.messages)


if __name__ == "__main__":
    ParametrizedFlow()
