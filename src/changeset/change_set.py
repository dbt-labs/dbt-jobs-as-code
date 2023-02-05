from typing import Optional
from pydantic import BaseModel
import string
from rich.table import Table


class Change(BaseModel):
    """Describes what a given change is and hot to apply it."""

    identifier: str
    type: str
    action: str
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

        for change in self.__root__:
            table.add_row(change.action.upper(), string.capwords(change.type), change.identifier)

        return table

    def __len__(self):
        return len(self.__root__)

    def apply(self):
        for change in self.__root__:
            change.apply()
