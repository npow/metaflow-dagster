from metaflow import FlowSpec, step, retry, timeout, environment


class RetryFlow(FlowSpec):
    @step
    def start(self):
        self.value = 1
        self.next(self.process)

    @retry(times=3, minutes_between_retries=2)
    @timeout(seconds=120)
    @environment(vars={"MY_VAR": "hello", "OTHER": "world"})
    @step
    def process(self):
        self.result = self.value * 2
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    RetryFlow()
