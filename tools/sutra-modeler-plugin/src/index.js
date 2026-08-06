/**
 * @startr-trade/sutra-modeler-plugin
 *
 * bpmn-js extension for modelling q:* BPMN elements (sutra q-namespace).
 *
 * Named exports:
 *   - qModdle             : moddle JSON descriptor (pass under moddleExtensions.q)
 *   - qPropertiesProvider : bpmn-js DI module (pass in additionalModules)
 */

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

import qPropertiesProvider from './properties/index.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const qModdle = JSON.parse(
  readFileSync(resolve(__dirname, './moddle/q-moddle.json'), 'utf8')
);

export { qModdle, qPropertiesProvider };
export default { qModdle, qPropertiesProvider };
