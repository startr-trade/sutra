/**
 * bpmn-js dependency-injection module — registers the QPropertiesProvider so that
 * `bpmn-js-properties-panel` will invoke its getGroups() for matching elements.
 */

import QPropertiesProvider from './QPropertiesProvider.js';

export default {
  __init__: [ 'qPropertiesProvider' ],
  qPropertiesProvider: [ 'type', QPropertiesProvider ]
};
