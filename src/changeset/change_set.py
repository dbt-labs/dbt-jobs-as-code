from typing import Optional
from pydantic import BaseModel
import string
from rich.table import Table


class Change(BaseModel):
    """Describes what a given change is and how to apply it."""

    identifier: str
    type: str
    action: str
    proj_id: int
    env_id: int
    sync_function: object
    parameters: dict

    def __str__(self):
        return f"{self.action.upper()} {string.capwords(self.type)} {self.identifier}"

    def apply(self):
        self.sync_function(**self.parameters)


class ChangeSet(BaseModel):
    """Store the set of changes to be displayed or applied."""

    __root__: Optional[list[Change]] = []

    def __iter__(self):
        return iter(self.__root__)

    def append(self, change: Change):
        self.__root__.append(change)

    def __str__(self):
        list_str = [str(change) for change in self.__root__]
        return "\n".join(list_str)

    def to_table(self) -> Table:
        """Return a table representation of the changeset."""

        table = Table(title="Changes detected")

        table.add_column("Action", style="cyan", no_wrap=True)
        table.add_column("Type", style="magenta")
        table.add_column("ID", style="green")
        table.add_column("Proj ID", style="yellow")
        table.add_column("Env ID", style="red")

        for change in self.__root__:
            table.add_row(
                change.action.upper(),
                string.capwords(change.type),
                change.identifier,
                str(change.proj_id),
                str(change.env_id),
            )

        return table

    def __len__(self):
        return len(self.__root__)

    def apply(self):
        for change in self.__root__:
            change.apply()
    
    def filter(self, environment_ids, project_ids):
        dbt_cloud_change_set = self.__root__
        dbt_cloud_change_set_filtered = ChangeSet()

        if len(environment_ids) != 0 or len(project_ids) != 0:
            for dbt_cloud_change in dbt_cloud_change_set:
                if dbt_cloud_change.env_id in environment_ids or dbt_cloud_change.proj_id in project_ids:
                    dbt_cloud_change_set_filtered.append(dbt_cloud_change)

            dbt_cloud_change_set = dbt_cloud_change_set_filtered

        return dbt_cloud_change_set