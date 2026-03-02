/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { fileURLToPath, URL } from 'node:url'
import { defineConfig, loadEnv, type PluginOption } from 'vite'
import vue from '@vitejs/plugin-vue'
import vueJsx from '@vitejs/plugin-vue-jsx'

// https://vitejs.dev/config/
export default defineConfig(async ({ command, mode }) => {
  const plugins: PluginOption[] = [vue(), vueJsx()]

  // Devtools plugin relies on browser globals (e.g. localStorage) and should not
  // be enabled during build (node environment).
  if (command === 'serve') {
    const { default: vueDevTools } = await import('vite-plugin-vue-devtools')
    plugins.push(vueDevTools())
  }

  return {
    base: '/',
    build: {
      outDir: '../seatunnel-engine-server/src/main/resources/ui'
    },
    plugins,
    resolve: {
      alias: {
        '@': fileURLToPath(new URL('./src', import.meta.url))
      }
    },
    server: {
      proxy: {
        '/api': {
          target: loadEnv(mode, './').VITE_APP_API_SERVICE,
          changeOrigin: true,
          rewrite: (path: string) => path.replace(/^\/api/, '')
        }
      }
    }
  }
})
