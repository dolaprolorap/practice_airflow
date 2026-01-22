from airflow.providers.standard.operators.branch import BaseBranchOperator
from settings import Settings

_settings = Settings()


class BranchOperator(BaseBranchOperator):
    def choose_branch(self, context):
        if _settings.skip_train:
            return 'skip_operator'

        return 'train_model_operator'
