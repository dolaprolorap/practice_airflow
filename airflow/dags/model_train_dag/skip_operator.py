from settings import Settings
from airflow.exceptions import AirflowSkipException
from airflow.sdk import task
from airflow.sdk import BaseOperator

_settings = Settings()


@task()
def skip(
        reason: str,
        skip_train: bool = _settings.skip_train
) -> None:
    if skip_train:
        raise AirflowSkipException(reason)


class SkipOperator(BaseOperator):
    def __init__(
            self,
            reason: str,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self._reason = reason

    def execute(self, context):
        raise AirflowSkipException(self._reason)
