#!/usr/env python3
from typing import Union, Iterable, Optional, Callable

from .event import Event, EventControlBlock, EventSystem
from .flag import FlagGroup


class StateEvent(Event):
    def __init__(
        self,
        name,
        current_state: Optional[FlagGroup] = None,
        secondary_state: Optional[
            Union[
                FlagGroup,
                Callable[[FlagGroup], FlagGroup],
                Callable[[FlagGroup, "StateMachine"], FlagGroup],
            ]
        ] = None,
        control_block: Optional[EventControlBlock] = None,
    ):
        super().__init__(name, control_block)

        self.current_state = current_state
        self.secondary_state = secondary_state


class StateMachine(EventSystem):
    """
    状态机
    ======
    基于 FlagGroup 与 EventSystem 实现的状态机
    """

    event_init = StateEvent("init", None, lambda cs, self: self._initial_state)

    def __init__(
        self,
        initial_state: Optional[FlagGroup] = None,
        final_state: Optional[FlagGroup] = None,
    ):
        super().__init__()

        self.state = None

        self._initial_state = initial_state
        self._final_state = final_state

        if self._initial_state is not None:
            self.init()

    def translation(self, event):
        pass

    def __getitem__(self, item):

        return super().__getitem__(item)

    def __getattr__(self, item):
        # 代理事件发生调用
        if item in self._event_dict:
            pass
        super().__getattribute__(item)
