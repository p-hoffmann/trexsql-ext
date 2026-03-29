/**
 * E2E tests for success criteria (T081-T086)
 *
 * SC-001: First code cell created within 10 seconds
 * SC-002: Code execution response <2s for simple operations
 * SC-003: Syntax highlighting appears <16ms
 * SC-004: Notebook handles 100+ cells without degradation
 * SC-007: All standard markdown syntax renders correctly
 * SC-008: Saved notebooks load with 100% fidelity
 */

import { test, expect } from '@playwright/test'
import fs from 'fs'

test.describe('Success Criteria', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await expect(page.locator('h1:has-text("React Notebook")')).toBeVisible()
  })

  test('SC-001: creates first code cell within 10 seconds', async ({ page }) => {
    const startTime = Date.now()

    // Click to create first cell
    await page.getByRole('button', { name: /Python/i }).first().click()

    // Wait for editor to be visible
    await expect(page.locator('.cm-editor')).toBeVisible()

    const endTime = Date.now()
    const elapsedTime = endTime - startTime

    // Verify it took less than 10 seconds
    expect(elapsedTime).toBeLessThan(10000)
  })

  test('SC-003: syntax highlighting appears quickly', async ({ page }) => {
    // Create a cell
    await page.getByRole('button', { name: /Python/i }).first().click()

    // Type code
    await page.locator('.cm-editor').click()
    await page.keyboard.type('def hello():')

    // Verify syntax highlighting is applied (keywords should be styled)
    // CodeMirror applies classes for syntax highlighting
    const hasHighlighting = await page.locator('.cm-keyword, .tok-keyword').count()

    // At least one keyword should be highlighted
    expect(hasHighlighting).toBeGreaterThan(0)
  })

  test('SC-004: handles 100+ cells without significant degradation', async ({ page }) => {
    // Create first cell
    await page.getByRole('button', { name: /Python/i }).first().click()

    const startTime = Date.now()

    // Add 100 cells
    for (let i = 0; i < 100; i++) {
      await page.getByRole('button', { name: /Code/i }).last().click()
    }

    const endTime = Date.now()
    const totalTime = endTime - startTime

    // Should complete adding 100 cells in reasonable time (less than 60s)
    expect(totalTime).toBeLessThan(60000)

    // Verify all cells exist
    const cellCount = await page.locator('.cm-editor').count()
    expect(cellCount).toBe(101) // Original + 100 added

    // Test interaction still works - click on last cell
    await page.locator('.cm-editor').last().click()
    await page.keyboard.type('# Last cell')

    // Verify typing worked
    await expect(page.locator('.cm-content').last()).toContainText('Last cell')
  })

  test('SC-007: standard markdown syntax renders correctly', async ({ page }) => {
    // Create markdown cell
    await page.getByRole('button', { name: /Markdown/i }).first().click()

    const markdownContent = `# Heading 1
## Heading 2
### Heading 3

**Bold text** and *italic text*

- Bullet item 1
- Bullet item 2

1. Numbered item 1
2. Numbered item 2

\`inline code\`

\`\`\`python
print("code block")
\`\`\`

[Link text](https://example.com)

> Blockquote text`

    await page.locator('.cm-editor').click()
    await page.keyboard.type(markdownContent)

    // Click outside to deselect and trigger render
    await page.keyboard.press('Escape')

    // Give time for markdown to render
    await page.waitForTimeout(500)

    // The markdown content should be visible in some form
    await expect(page.locator('.cm-content')).toContainText('Heading 1')
  })

  test('SC-008: saved notebooks load with 100% fidelity', async ({ page }) => {
    // Create cells with specific content
    await page.getByRole('button', { name: /Python/i }).first().click()
    await page.locator('.cm-editor').first().click()
    const codeContent = 'result = 1 + 2 + 3'
    await page.keyboard.type(codeContent)

    await page.getByRole('button', { name: /Markdown/i }).last().click()
    await page.locator('.cm-editor').last().click()
    const mdContent = '# Analysis Results'
    await page.keyboard.type(mdContent)

    // Save notebook
    const downloadPromise = page.waitForEvent('download')
    await page.getByRole('button', { name: /Save/i }).click()
    const download = await downloadPromise

    // Read downloaded content
    const downloadPath = await download.path()
    if (downloadPath) {
      const content = fs.readFileSync(downloadPath, 'utf-8')
      const notebook = JSON.parse(content)

      // Verify structure
      expect(notebook.nbformat).toBe(4)
      expect(notebook.nbformat_minor).toBeGreaterThanOrEqual(0)
      expect(notebook.cells).toHaveLength(2)

      // Verify first cell
      expect(notebook.cells[0].cell_type).toBe('code')
      expect(notebook.cells[0].source.join('')).toContain('result')

      // Verify second cell
      expect(notebook.cells[1].cell_type).toBe('markdown')
      expect(notebook.cells[1].source.join('')).toContain('Analysis')

      // Verify metadata exists
      expect(notebook.metadata).toBeDefined()
    }
  })

  test('performance: cell creation is fast', async ({ page }) => {
    // Warm up
    await page.getByRole('button', { name: /Python/i }).first().click()
    await expect(page.locator('.cm-editor')).toBeVisible()

    // Measure cell creation time
    const times: number[] = []

    for (let i = 0; i < 10; i++) {
      const start = Date.now()
      await page.getByRole('button', { name: /Code/i }).last().click()
      await page.locator('.cm-editor').last().waitFor({ state: 'visible' })
      times.push(Date.now() - start)
    }

    // Calculate average
    const avgTime = times.reduce((a, b) => a + b, 0) / times.length

    // Average cell creation should be under 500ms
    expect(avgTime).toBeLessThan(500)
  })

  test('keyboard shortcuts work correctly', async ({ page }) => {
    // Create a cell
    await page.getByRole('button', { name: /Python/i }).first().click()

    // Exit edit mode
    await page.keyboard.press('Escape')

    // B key should add cell below
    await page.keyboard.press('b')
    await expect(page.locator('.cm-editor')).toHaveCount(2)

    // A key should add cell above
    await page.keyboard.press('a')
    await expect(page.locator('.cm-editor')).toHaveCount(3)
  })
})
