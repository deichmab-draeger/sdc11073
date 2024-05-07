from __future__ import annotations

from sdc11073.provider.providerimpl import SdcProvider


class SdcProviderEntity(SdcProvider):
    """A sdc provider that only uses the entity interface of mdib."""

    def _setup_components(self):
        self._subscriptions_managers = {}
        for name, cls in self._components.subscriptions_manager_class.items():
            mgr = cls(self._mdib.sdc_definitions,
                      self.msg_factory,
                      self._soap_client_pool,
                      self._max_subscription_duration,
                      log_prefix=self._log_prefix,
                      )
            self._subscriptions_managers[name] = mgr

        services_factory = self._components.services_factory
        self.hosted_services = services_factory(self, self._components, self._subscriptions_managers)
        for dpws_service in self.hosted_services.dpws_hosted_services.values():
            self._hosted_service_dispatcher.register_instance(dpws_service.path_element, dpws_service)

        cls = self._components.sco_operations_registry_class
        pm_names = self._mdib.data_model.pm_names

        sco_descr_entities = self._mdib.entities.NODETYPE.get(pm_names.ScoDescriptor, [])
        for sco_descr_entity in sco_descr_entities:
            sco_descr = sco_descr_entity.descriptor
            sco_operations_registry = cls(self.hosted_services.set_service,
                                          self._components.operation_cls_getter,
                                          self._mdib,
                                          sco_descr,
                                          log_prefix=self._log_prefix)
            self._sco_operations_registries[sco_descr.Handle] = sco_operations_registry

            product_roles = self._components.role_provider_class(self._mdib,
                                                                 sco_operations_registry,
                                                                 self._log_prefix)
            self.product_lookup[sco_descr.Handle] = product_roles
            product_roles.init_operations()
        if self._components.waveform_provider_class is not None:
            self.waveform_provider = self._components.waveform_provider_class(self._mdib,
                                                                              self._log_prefix)

        # product roles might have added descriptors, set source mds for all
        self._mdib.xtra.set_all_source_mds()
