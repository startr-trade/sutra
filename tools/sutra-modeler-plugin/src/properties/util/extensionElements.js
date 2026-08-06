/**
 * Helpers to read/write q:* extension elements on a bpmn:* business object.
 *
 * Kept dependency-light so unit tests can exercise these without a full bpmn-js bootstrap.
 */

import { getBusinessObject } from 'bpmn-js/lib/util/ModelUtil';

export function getExtensionElement(element, type) {
  const bo = getBusinessObject(element);
  const extensions = bo && bo.extensionElements;
  if (!extensions || !extensions.values) {
    return undefined;
  }
  return extensions.values.find((v) => v.$type === type);
}

export function getAllExtensionElements(element, type) {
  const bo = getBusinessObject(element);
  const extensions = bo && bo.extensionElements;
  if (!extensions || !extensions.values) {
    return [];
  }
  return extensions.values.filter((v) => v.$type === type);
}
