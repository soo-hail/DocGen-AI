from typing import Callable, Iterable, TypeVar, Union

IterableItemType = TypeVar('IterableItemType')

# 'pred' is a function that takes element of iterable and returns bool-value, 'iterable' is a collection-variable(eg: List, Tuple, Set, Dict).
def find(pred: Callable[[IterableItemType], bool], iterable: Iterable[IterableItemType]) -> Union[IterableItemType, None]:
    for ele in iterable:
        if pred(ele):
            return ele

    return None