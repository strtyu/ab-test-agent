class ABAgentError(Exception):
    pass


class ValidationError(ABAgentError):
    pass


class BQQueryError(ABAgentError):
    pass


class BQDryRunError(BQQueryError):
    def __init__(self, message: str, sql: str = ""):
        super().__init__(message)
        self.sql = sql


class AgentError(ABAgentError):
    pass


class MetricNotFoundError(ABAgentError):
    def __init__(self, metric_name: str):
        super().__init__(f"Metric '{metric_name}' not found in registry")
        self.metric_name = metric_name


class MetricValidationError(ABAgentError):
    pass


class SlackError(ABAgentError):
    pass


class CodeExecutionError(ABAgentError):
    pass
