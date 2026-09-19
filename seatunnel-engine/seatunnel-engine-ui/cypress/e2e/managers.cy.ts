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

describe('Worker resources', () => {
  beforeEach(() => {
    cy.intercept('GET', '**/overview', {
      projectVersion: 'test',
      gitCommitAbbrev: 'test',
      totalSlot: '4',
      unassignedSlot: '1',
      workers: '2',
      runningJobs: '1'
    })
    cy.intercept('GET', '**/system-monitoring-information', [
      {
        host: 'fixed',
        port: '5801',
        isMaster: 'false',
        'load.process': '10%',
        'heap.memory.used': '1G',
        'heap.memory.max': '4G',
        'thread.count': '12'
      },
      { host: '2001:db8::1', port: '5801', isMaster: 'false' },
      { host: 'master', port: '5801', isMaster: 'true' }
    ]).as('monitoring')
    cy.intercept('GET', '**/resource/workers', {
      available: true,
      collectedAt: 1723017600000,
      workers: [
        {
          address: 'fixed:5801',
          dynamicSlot: false,
          totalSlots: 4,
          usedSlots: 3,
          freeSlots: 1,
          availableCpuCores: 0,
          totalCpuCores: 8,
          cpuUsage: 0,
          tags: { region: '<img src=x onerror=alert(1)>' },
          runningJobIds: []
        },
        {
          address: '[2001:db8::1]:5801',
          dynamicSlot: true,
          totalSlots: 999,
          usedSlots: 2,
          freeSlots: 997
        }
      ]
    }).as('resources')
  })

  it('shows fixed and dynamic slots and read-only details', () => {
    cy.visit('/#/managers/workers')
    cy.wait(['@monitoring', '@resources'])
    cy.contains('3 / 4 used, 1 free')
    cy.contains('Dynamic — 2 used (no fixed capacity)')
    cy.contains('999').should('not.exist')
    cy.contains('td', 'fixed:5801').parent().contains('button', 'Details').click()
    cy.get('.n-drawer')
      .should('be.visible')
      .within(() => {
        cy.contains('0 / 8')
        cy.contains('0.0%')
        cy.contains('heap.memory.max')
        cy.contains('<img src=x onerror=alert(1)>')
        cy.get('img').should('not.exist')
      })
  })

  it('clears previous slots on an unavailable snapshot and recovers', () => {
    cy.visit('/#/managers/workers')
    cy.wait(['@monitoring', '@resources'])
    cy.contains('3 / 4 used, 1 free')
    cy.intercept('GET', '**/resource/workers', {
      available: false,
      collectedAt: 1723017610000,
      workers: []
    }).as('unavailable')
    cy.contains('button', 'Refresh').click()
    cy.wait('@unavailable')
    cy.contains('Worker resources are unavailable')
    cy.contains('3 / 4 used, 1 free').should('not.exist')
    cy.contains('fixed:5801')
    cy.intercept('GET', '**/resource/workers', {
      available: true,
      collectedAt: 1723017620000,
      workers: [
        { address: 'fixed:5801', dynamicSlot: false, totalSlots: 4, usedSlots: 0, freeSlots: 4 }
      ]
    }).as('recovered')
    cy.contains('button', 'Refresh').click()
    cy.wait('@recovered')
    cy.contains('0 / 4 used, 4 free')
    cy.contains('Worker resources are unavailable').should('not.exist')
  })

  it('does not fetch worker slots on the Master page', () => {
    cy.visit('/#/managers/master')
    cy.wait('@monitoring')
    cy.contains('master:5801')
    cy.contains('th', 'Slots').should('not.exist')
    cy.get('@resources.all').should('have.length', 0)
  })
})
