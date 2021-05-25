from ..namespaces import domTag
from .. pmtypes import ComponentActivation
from .providerbase import ProviderRole


class GenericMetricProvider(ProviderRole):
    """ Always added operations: None
    This is a generic Handler for
    - SetValueOperation on numeric metrics
    - SetStringOperation on (enum) string metrics
    """
    def __init__(self, activationStateCanRemoveMetricValue=True, log_prefix=None):
        '''

        :param activationStateCanRemoveMetricValue: if True, SF717 is handled
               SF717: A Metric Provider shall not provide a MetricValue if the ActivationState = Shtdn|Off|Fail.
        '''
        super().__init__(log_prefix)
        self.activationStateCanRemoveMetricValue = activationStateCanRemoveMetricValue

    def makeOperationInstance(self, operationDescriptorContainer, operations_factory):
        ''' Can handle following cases:
        SetValueOperation, target = NumericMetricDescriptor: => handler = _setNumericValue
        SetStringOperation, target = (Enum)StringMetricDescriptor: => handler = _setString
        SetMetricStateOperationDescriptor, target = any subclass of AbstractMetricDescriptor: => handler = _setMetricState
        '''
        operationTargetHandle = operationDescriptorContainer.OperationTarget
        operationTargetDescriptorContainer = self._mdib.descriptions.handle.getOne(operationTargetHandle)

        if operationTargetDescriptorContainer.NODETYPE not in (domTag('StringMetricDescriptor'),
                                                               domTag('EnumStringMetricDescriptor'),
                                                               domTag('NumericMetricDescriptor'),
                                                               domTag('RealTimeSampleArrayMetricDescriptor')):
            return None # this is not metric provider role

        if operationDescriptorContainer.NODETYPE == domTag('SetValueOperationDescriptor'):
            if operationTargetDescriptorContainer.NODETYPE == domTag('NumericMetricDescriptor'):
                op_cls = operations_factory(domTag('SetValueOperationDescriptor'))
                return self._mkOperation(op_cls,
                                         handle=operationDescriptorContainer.handle,
                                         operationTargetHandle=operationTargetHandle,
                                         codedValue=operationDescriptorContainer.Type,
                                         currentArgumentHandler=self._setNumericValue)
            return None
        elif operationDescriptorContainer.NODETYPE == domTag('SetStringOperationDescriptor'):
            if operationTargetDescriptorContainer.NODETYPE in (domTag('StringMetricDescriptor'),
                                                               domTag('EnumStringMetricDescriptor')):
                op_cls = operations_factory(domTag('SetStringOperationDescriptor'))
                return self._mkOperation(op_cls,
                                         handle=operationDescriptorContainer.handle,
                                         operationTargetHandle=operationTargetHandle,
                                         codedValue=operationDescriptorContainer.Type,
                                         currentArgumentHandler=self._setString)
            return None
        elif operationDescriptorContainer.NODETYPE == domTag('SetMetricStateOperationDescriptor'):
            op_cls = operations_factory(domTag('SetMetricStateOperationDescriptor'))
            operation = self._mkOperation(op_cls,
                                          handle=operationDescriptorContainer.handle,
                                          operationTargetHandle=operationTargetHandle,
                                          codedValue=operationDescriptorContainer.Type,
                                          currentArgumentHandler=self._setMetricState)
            return operation
        return None

    def _setMetricState(self, operationInstance, value):
        '''

        :param operationInstance: the operation
        :param value: a list of proposed metric states
        :return:
        '''
        #ToDo: consider ModifiableDate attribute
        operationInstance.currentValue = value
        with self._mdib.mdibUpdateTransaction() as mgr:
            for proposedMetricState in value:
                state = mgr.getMetricState(proposedMetricState.descriptorHandle)
                if state.isMetricState:
                    self._logger.info('updating {} with proposed metric state', state)
                    state.update_from_other_container(proposedMetricState,
                                                      skipped_properties=['StateVersion', 'DescriptorVersion'])
                else:
                    self._logger.warn('_setMetricState operation: ignore invalid referenced type {} in operation', state.NODETYPE)

    def onPreCommit(self, mdib, transaction):
        if not self.activationStateCanRemoveMetricValue:
            return
        if transaction.metricStateUpdates:
            self._handleMetricsComponentActivation(transaction.metricStateUpdates.values())
        if transaction.rtSampleStateUpdates:
            self._handleMetricsComponentActivation(transaction.rtSampleStateUpdates.values())

    def _handleMetricsComponentActivation(self, metricStateUpdates):
        # check if MetricValue shall be removed
        for oldstate, newstate in metricStateUpdates:
            if newstate is None or not newstate.isMetricState:
                continue
            # SF717: check if MetricValue shall be automatically removed
            if newstate.ActivationState in (ComponentActivation.OFF,
                                            ComponentActivation.SHUTDOWN,
                                            ComponentActivation.FAILURE):
                if newstate.metricValue is not None:
                    # remove metric value
                    self._logger.info('{}: remove metric value because ActivationState="{}", handle="{}"',
                                      self.__class__.__name__, newstate.ActivationState, newstate.descriptorHandle)
                    newstate.metricValue = None
