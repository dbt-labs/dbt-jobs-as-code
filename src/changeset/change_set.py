from typing import Optional
from pydantic import BaseModel

class Change(BaseModel):
    """Describes what a given change is and hot to apply it."""
    identifier: str
    type: str
    action: str
    sync_function: object
    parameters: dict

    def __str__(self):
        return f"{self.action.upper()} {self.type.capitalize()} {self.identifier}"

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
    
    def __len__(self):
        return len(self.__root__)

    def apply(self):
        for change in self.__root__:
            change.apply()