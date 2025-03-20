from typing import Any, Union

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from lakefs_provider.hooks.lakefs_hook import LakeFSHook # type: ignore

class LakeFSSenseAndGetCommitOperator(BaseSensorOperator):
    """
    Senses for a commit on a branch and then returns the commit details.

    :param lakefs_conn_id: The connection to run the sensor against
    :type lakefs_conn_id: str
    :param repo: The lakeFS repo.
    :type repo: str
    :param branch: The branch to sense for.
    :type branch: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'repo',
        'branch',
    ]

    current_commit_id_key = 'current_commit_id'
    branch_not_found_error = "Resource Not Found"

    @apply_defaults
    def __init__(self, lakefs_conn_id: str, repo: str, branch: str, prev_commit_id: Union[str, None] = None,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.lakefs_conn_id = lakefs_conn_id
        self.repo = repo
        self.branch = branch
        self.prev_commit_id = prev_commit_id
        self.hook = LakeFSHook(lakefs_conn_id)

    def poke(self, context):
        if self.prev_commit_id is None:
            self.prev_commit_id = context.get(self.current_commit_id_key, None)
            if self.prev_commit_id is None:
                commit_details, exists = self.get_commit()
                if exists and commit_details:
                    self.prev_commit_id = commit_details.get('id')
                return False

        self.log.info('Poking: branch %s on repo %s', self.branch, self.repo)
        commit_details, exists = self.get_commit()
        if not exists:
            return False
        assert commit_details
        curr_commit_id = commit_details.get('id')
        if not curr_commit_id:
            return False
        
        self.log.info('Previous ref: %s, current ref %s', self.prev_commit_id, curr_commit_id)
        return curr_commit_id != self.prev_commit_id
    
    def execute(self, context) -> Any:
        self.log.info("Waiting for a new commit on branch '%s' of repo '%s'.", self.branch, self.repo)
        result = super().execute(context)
        commit_details, exists = self.get_commit()
        if exists and commit_details:
            self.log.info("Returning commit details of id: %s", commit_details.get('id'))
            return commit_details  # This value is pushed to XCom.
        else:
            raise ValueError("Commit details not found after sensor triggered.")
    
    def get_commit(self):
        try:
            commit_details = self.hook.get_commit(self.repo, self.branch)
        except Exception:  # Catch a more specific exception if available
            self.log.info("Branch '%s' not found in repo '%s'", self.branch, self.repo)
            return None, False
        return commit_details, True
