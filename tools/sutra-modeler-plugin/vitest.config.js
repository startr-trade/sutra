import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'jsdom',
    globals: false,
    include: [ 'src/__tests__/**/*.test.js' ],
    setupFiles: [ './src/__tests__/setup.js' ]
  }
});
