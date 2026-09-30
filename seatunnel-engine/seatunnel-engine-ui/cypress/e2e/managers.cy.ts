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
          address: '[fixed]:5801',
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

  it('pauses hidden-tab polling and refreshes when visible again', () => {
    cy.clock(0, ['setTimeout', 'clearTimeout'])
    cy.visit('/#/managers/workers', {
      onBeforeLoad(win) {
        const requests = cy.spy(win.XMLHttpRequest.prototype, 'open')
        requests
          .withArgs('GET', Cypress.sinon.match(/\/system-monitoring-information$/))
          .as('monitoringStarts')
        requests.withArgs('GET', Cypress.sinon.match(/\/resource\/workers$/)).as('resourceStarts')
      }
    })
    cy.wait(['@monitoring', '@resources'])
    cy.contains('button', 'Refresh').should('not.be.disabled')
    // Prove the real page's polling timer is armed before exercising the pause.
    cy.tick(30_000)
    cy.wait(['@monitoring', '@resources'])
    cy.contains('button', 'Refresh').should('not.be.disabled')
    cy.get('@monitoringStarts').should('have.been.calledTwice')
    cy.get('@resourceStarts').should('have.been.calledTwice')
    cy.document().then((doc) => {
      Object.defineProperty(doc, 'visibilityState', { configurable: true, get: () => 'hidden' })
      doc.dispatchEvent(new Event('visibilitychange'))
    })
    cy.tick(90_000)
    // Let timer-triggered Axios promise chains drain before reading page-side calls.
    // Unlike proxy alias counts, these observe request starts without network latency.
    cy.window().then(
      (win) =>
        new Cypress.Promise<void>((resolve) => {
          const channel = new win.MessageChannel()
          channel.port1.onmessage = () => {
            channel.port1.close()
            channel.port2.close()
            resolve()
          }
          channel.port2.postMessage(null)
        })
    )
    cy.get('@monitoringStarts').should('have.been.calledTwice')
    cy.get('@resourceStarts').should('have.been.calledTwice')
    cy.document().then((doc) => {
      Object.defineProperty(doc, 'visibilityState', { configurable: true, get: () => 'visible' })
      doc.dispatchEvent(new Event('visibilitychange'))
    })
    cy.wait(['@monitoring', '@resources'])
    cy.contains('button', 'Refresh').should('not.be.disabled')
    cy.get('@monitoringStarts').should('have.been.calledThrice')
    cy.get('@resourceStarts').should('have.been.calledThrice')
    cy.contains('3 / 4 used, 1 free')
  })

  it('does not fetch worker slots on the Master page', () => {
    cy.visit('/#/managers/master')
    cy.wait('@monitoring')
    cy.contains('master:5801')
    cy.contains('th', 'Slots').should('not.exist')
    cy.get('@resources.all').should('have.length', 0)
  })

  for (const width of [1440, 768, 390]) {
    it(`keeps worker cells readable while scrolling at ${width}px`, () => {
      cy.viewport(width, 900)
      cy.visit('/#/managers/workers')
      cy.wait(['@monitoring', '@resources'])
      cy.get('.n-data-table .n-scrollbar-container').as('tableViewport')
      cy.get('@tableViewport').should(($viewport) => {
        expect($viewport[0].scrollWidth).to.be.greaterThan($viewport[0].clientWidth)
      })
      // At the left edge an off-screen action must not cover the address column.
      cy.get('@tableViewport').scrollTo('left')
      cy.contains('td', 'fixed:5801').should('be.visible')
      cy.get('.n-data-table tbody tr')
        .first()
        .find('td')
        .last()
        .should(($action) => {
          const action = $action[0]
          const viewport = action.closest('.n-scrollbar-container')!
          expect(action.getBoundingClientRect().right).to.be.greaterThan(
            viewport.getBoundingClientRect().right
          )
        })
      cy.get('@tableViewport').scrollTo('right')
      cy.contains('td', 'Dynamic — 2 used (no fixed capacity)').should(($cell) => {
        const cell = $cell[0]
        const action = cell.nextElementSibling!
        const bounds = cell.getBoundingClientRect()
        const range = cell.ownerDocument.createRange()
        range.selectNodeContents(cell.firstElementChild || cell)
        const lines = Array.from(range.getClientRects()).filter((rect) => rect.width > 0)
        expect(lines.length, 'wrapped slot text').to.be.greaterThan(1)
        for (const line of lines) {
          expect(line.left, 'text stays inside its cell').to.be.at.least(bounds.left)
          expect(line.right, 'text stays before the next cell').to.be.at.most(bounds.right)
        }
        expect(
          action.getBoundingClientRect().left,
          'action does not overlap slot cell'
        ).to.be.at.least(bounds.right - 1)
      })
      cy.get('.n-data-table tbody tr').first().contains('button', 'Details').click()
      cy.get('.n-drawer').should('be.visible').and('contain', 'fixed:5801')
      if (width === 390) {
        cy.get('.n-drawer-header__close').click()
        cy.get('.n-drawer').should('not.exist')
        cy.get('.n-layout-toggle-bar').click()
        cy.get('.n-layout-sider').should(($sidebar) => {
          expect($sidebar[0].getBoundingClientRect().width).to.be.at.most(65)
        })
        cy.contains('td', 'Dynamic — 2 used (no fixed capacity)').scrollIntoView()
        cy.contains('td', 'Dynamic — 2 used (no fixed capacity)').then(($cell) => {
          cy.get('@tableViewport').scrollTo($cell[0].offsetLeft, 0)
        })
        cy.contains('td', 'Dynamic — 2 used (no fixed capacity)').should(($cell) => {
          const cell = $cell[0]
          const viewport = cell.closest('.n-scrollbar-container')!.getBoundingClientRect()
          const range = cell.ownerDocument.createRange()
          range.selectNodeContents(cell.firstElementChild || cell)
          for (const line of Array.from(range.getClientRects())) {
            expect(line.left, 'slot text visible after sidebar collapse').to.be.at.least(
              viewport.left
            )
            expect(line.right, 'slot text visible after sidebar collapse').to.be.at.most(
              viewport.right
            )
          }
        })
      }
    })
  }
})
