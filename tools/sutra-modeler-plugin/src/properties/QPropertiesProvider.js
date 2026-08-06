/**
 * QPropertiesProvider — registers property panel groups for all q:* elements.
 *
 * Surface (aligned with the frozen xsd/q.xsd):
 *
 *   - <q:source>        on bpmn:StartEvent    (channel, ack, dedupKey, type, dataClass)
 *   - <q:input>         on bpmn:StartEvent    (name, codec, accept, validators chain)
 *   - <q:onValidation>  on bpmn:StartEvent    (mode, errorCode)
 *   - <q:alias>         on bpmn:StartEvent    (expression, onConflict, multi)
 *   - <q:reply>         on bpmn:EndEvent      (mode, destination, contentType, …)
 *   - <q:dispatch>      on bpmn:CallActivity  (default, onNoMatch)
 *   - <q:case>          on bpmn:CallActivity  (ordered when/calledElement list)
 *   - <q:audit>         on bpmn:Process AND any flow node (flow-node overlays override
 *                       the process-level defaults; `target` overrides eventType)
 *
 * Singular <q:validator> is intentionally unsupported — per xsd/q.xsd the schema's
 * InputType sequence only references the plural <q:validators source="…"> element, so a
 * <q:validator> child cannot land in a conforming BPMN file. The plural collection is
 * edited inline within QInputGroup.
 *
 * Provider follows the bpmn-js-properties-panel@5 contract:
 *   getGroups(element) => (groups) => groups
 */

import { is } from 'bpmn-js/lib/util/ModelUtil';

import { QSourceGroup } from './groups/QSourceGroup.js';
import { QInputGroup } from './groups/QInputGroup.js';
import { QOnValidationGroup } from './groups/QOnValidationGroup.js';
import { QAliasGroup } from './groups/QAliasGroup.js';
import { QReplyGroup } from './groups/QReplyGroup.js';
import { QDispatchGroup } from './groups/QDispatchGroup.js';
import { QCaseGroup } from './groups/QCaseGroup.js';
import { QAuditGroup } from './groups/QAuditGroup.js';

const LOW_PRIORITY = 500;

// Flow-node types that may carry a <q:audit> overlay (per the moddle's `allowedIn`).
const AUDIT_FLOW_NODE_TYPES = [
  'bpmn:ServiceTask',
  'bpmn:Task',
  'bpmn:UserTask',
  'bpmn:EndEvent',
  'bpmn:CallActivity',
  'bpmn:ExclusiveGateway'
];

function isAuditableFlowNode(element) {
  return AUDIT_FLOW_NODE_TYPES.some((t) => is(element, t));
}

export default class QPropertiesProvider {

  constructor(propertiesPanel, translate) {
    this._translate = translate;
    propertiesPanel.registerProvider(LOW_PRIORITY, this);
  }

  getGroups(element) {
    return (groups) => {
      if (is(element, 'bpmn:StartEvent')) {
        groups.push(QSourceGroup(element, this._translate));
        groups.push(QInputGroup(element, this._translate));
        groups.push(QOnValidationGroup(element, this._translate));
        groups.push(QAliasGroup(element, this._translate));
      }
      if (is(element, 'bpmn:EndEvent')) {
        groups.push(QReplyGroup(element, this._translate));
      }
      if (is(element, 'bpmn:CallActivity')) {
        groups.push(QDispatchGroup(element, this._translate));
        groups.push(QCaseGroup(element, this._translate));
      }
      if (is(element, 'bpmn:Process') || isAuditableFlowNode(element)) {
        groups.push(QAuditGroup(element, this._translate));
      }
      return groups;
    };
  }
}

QPropertiesProvider.$inject = [ 'propertiesPanel', 'translate' ];
