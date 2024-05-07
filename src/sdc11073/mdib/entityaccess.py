from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable
    from .descriptorcontainers import AbstractDescriptorContainer
    from .statecontainers import AbstractMultiStateContainer, AbstractStateContainer
    from .mdibbase import DescriptorsLookup, StatesLookup, MultiStatesLookup


class EntityBase:
    """Groups descriptor and state."""
    is_multi_state = False

    def __init__(self, descriptor: AbstractDescriptorContainer):
        self.descriptor = descriptor

    def __str__(self):
        return f'{self.__class__.__name__} {self.descriptor.NODETYPE.localname} handle {self.descriptor.Handle}'

    @property
    def parent_handle(self) -> str:
        return self.descriptor.parent_handle


class Entity(EntityBase):
    """Groups descriptor and state."""

    def __init__(self, descriptor: AbstractDescriptorContainer,
                 state: AbstractStateContainer | None = None):
        super().__init__(descriptor)
        self.state = state

    def clear_states(self):
        """Remove state from entity."""
        self.state = None

    def add_state(self, state_container: AbstractStateContainer):
        """Set the state of entity."""
        if self.state is not None:
            raise ValueError(f'entity {self.descriptor.Handle} already has a state')
        self.state = state_container

    @property
    def state_handles(self) ->  Iterable[str]:
        """Return the handles of all multi state states (always empty)."""
        return []


class MultiStateEntity(EntityBase):
    """Groups descriptor and list of multi-states."""

    is_multi_state = True

    def __init__(self, descriptor: AbstractDescriptorContainer):
        super().__init__(descriptor)
        self.states: dict[str, AbstractMultiStateContainer] = {}

    def clear_states(self):
        """Remove all states from entity."""
        self.states.clear()

    def add_state(self, state_container: AbstractMultiStateContainer):
        """Add a state to entity."""
        if state_container.Handle in self.states:
            # Todo: handle context state update
            raise ValueError(f'state {state_container.Handle} already present, todo: fix this!')
        self.states[state_container.Handle] = state_container

    @property
    def state_handles(self) -> Iterable[str]:
        """Return the handles of all multi state states."""
        return self.states.keys()


class _Getter:
    def __init__(self,
                 entity_access: _EntityAccess,
                 idx):
        self._entity_access = entity_access
        self._idx = idx

    def get_one(self, key: Any, allow_none: bool = False) -> Entity | MultiStateEntity | None:
        descriptor = self._idx.get_one(key, allow_none)
        if descriptor is None:
            return None

        return self._entity_access.get_entity(descriptor)

    def get(self, key: Any, default: Any = None) -> list[Entity | MultiStateEntity] | Any:
        descriptors = self._idx.get(key, default)
        if descriptors is None:
            return default
        return [self._entity_access.get_entity(d) for d in descriptors]


class _CtxtStateGetter:
    def __init__(self,
                 entity_access: _EntityAccess,
                 idx):
        self._entity_access = entity_access
        self._idx = idx

    def get_one(self, key: Any, allow_none: bool = False) -> Entity | MultiStateEntity | None:
        state = self._idx.get_one(key, allow_none)
        if state is None:
            return None
        descriptor = self._entity_access.descriptors_lookup.get_one(state.DescriptorHandle)
        return self._entity_access.get_entity(descriptor)

    # def get(self, key: Any, default: Any = None) -> list[Entity | MultiStateEntity] | Any:
    #     descriptors = self._idx.get(key, default)
    #     if descriptors is None:
    #         return default
    #     return [self._entity_access.get_entity(d) for d in descriptors]


class EntityAccess:
    def __init__(self,
                 descriptors_lookup: DescriptorsLookup,
                 states_lookup: StatesLookup,
                 context_states_lookup: MultiStatesLookup):
        self.descriptors_lookup = descriptors_lookup
        self.states_lookup = states_lookup
        self.context_states_lookup = context_states_lookup
        self.entity_cache: dict[str, Entity | MultiStateEntity] = {}

    @property
    def NODETYPE(self) -> _Getter:
        return _Getter(self, self.descriptors_lookup.NODETYPE)

    @property
    def handle(self) -> _Getter:
        return _Getter(self, self.descriptors_lookup.handle)

    @property
    def parent_handle(self) -> _Getter:
        return _Getter(self, self.descriptors_lookup.parent_handle)

    @property
    def state_handle(self) -> _CtxtStateGetter:
        return _CtxtStateGetter(self, self.context_states_lookup.handle)

    @property
    def condition_signaled(self) -> _Getter:
        return _Getter(self, self.descriptors_lookup.condition_signaled)

    @property
    def coding(self) -> _Getter:
        return _Getter(self, self.descriptors_lookup.coding)

    def add_containers(self, descr: AbstractDescriptorContainer, state: AbstractStateContainer | None):
        """Append descr and state with locking."""
        self.descriptors_lookup.add_object(descr)
        if state:
            self.states_lookup.add_object(state)

    def add_multi_state_containers(self, descr: AbstractDescriptorContainer, states: list[AbstractMultiStateContainer]):
        """Append descr and states with locking."""
        self.descriptors_lookup.add_object(descr)
        for state in states:
            self.context_states_lookup.add_object(state)

    def update_object(self, entity: Entity | MultiStateEntity):
        self.descriptors_lookup.update_object(entity.state)
        if entity.is_multi_state:
            for state in entity.states.values():
                self.context_states_lookup.update_object(state)
        else:
            self.states_lookup.update_object(entity.state)

    @property
    def objects(self):
        """Return entities, created on the fly (not stored in mdib!)."""
        for descr in self.descriptors_lookup.objects:
            yield self.get_entity(descr)

    def get_entity(self, descr: AbstractDescriptorContainer) -> Entity | MultiStateEntity:
        entity = self.entity_cache.get(descr.Handle)
        if entity is not None:
            return entity

        if descr.is_context_descriptor:
            entity = MultiStateEntity(descr)
            for state in self.context_states_lookup.descriptor_handle.get(descr.Handle, []):
                entity.add_state(state)
        else:
            state = self.states_lookup.descriptor_handle.get_one(descr.Handle,allow_none = True)
            entity = Entity(descr, state)
        self.entity_cache[descr.Handle] = entity
        return entity

