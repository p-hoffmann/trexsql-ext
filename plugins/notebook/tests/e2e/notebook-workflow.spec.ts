/**
 * E2E tests for notebook import/export workflow (T063, T088)
 */

import { test, expect } from '@playwright/test'
import fs from 'fs'

test.describe('Notebook Workflow', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await expect(page.locator('h1:has-text("React Notebook")')).toBeVisible()
  })

  test('creates and saves a notebook', async ({ page }) => {
    // Create a cell with content
    await page.getByRole('button', { name: /Python/i }).first().click()
    await page.locator('.cm-editor').click()
    await page.keyboard.type('print("Hello from E2E test")')

    // Add a markdown cell
    await page.getByRole('button', { name: /Markdown/i }).last().click()
    await page.locator('.cm-editor').last().click()
    await page.keyboard.type('# Test Heading')

    // Set up download listener
    const downloadPromise = page.waitForEvent('download')

    // Click Save button
    await page.getByRole('button', { name: /Save/i }).click()

    // Wait for download
    const download = await downloadPromise
    expect(download.suggestedFilename()).toContain('.ipynb')
  })

  test('creates a new notebook', async ({ page }) => {
    // Create a cell first
    await page.getByRole('button', { name: /Python/i }).first().click()
    await page.locator('.cm-editor').click()
    await page.keyboard.type('some code')

    // Set up dialog handler for confirmation
    page.on('dialog', (dialog) => dialog.accept())

    // Click New button
    await page.getByRole('button', { name: /New/i }).click()

    // Notebook should be empty
    await expect(page.locator('.cm-editor')).not.toBeVisible()
    await expect(page.getByText(/Add your first cell/i)).toBeVisible()
  })

  test('complete notebook workflow: create, edit, save, reload', async ({ page }) => {
    // Step 1: Create cells
    await page.getByRole('button', { name: /Python/i }).first().click()
    await page.locator('.cm-editor').first().click()
    await page.keyboard.type('x = 1 + 2')

    await page.getByRole('button', { name: /Markdown/i }).last().click()
    await page.locator('.cm-editor').last().click()
    await page.keyboard.type('## Results')

    // Step 2: Verify cells exist
    await expect(page.locator('.cm-editor')).toHaveCount(2)

    // Step 3: Save the notebook
    const downloadPromise = page.waitForEvent('download')
    await page.getByRole('button', { name: /Save/i }).click()
    const download = await downloadPromise

    // Step 4: Get downloaded content
    const downloadPath = await download.path()
    if (downloadPath) {
      const content = fs.readFileSync(downloadPath, 'utf-8')
      const notebook = JSON.parse(content)

      // Verify notebook structure
      expect(notebook.nbformat).toBe(4)
      expect(notebook.cells).toHaveLength(2)
      expect(notebook.cells[0].cell_type).toBe('code')
      expect(notebook.cells[1].cell_type).toBe('markdown')
    }
  })

  test('handles empty notebook gracefully', async ({ page }) => {
    // Initially empty
    await expect(page.getByText(/Add your first cell/i)).toBeVisible()

    // Download should still work
    const downloadPromise = page.waitForEvent('download')
    await page.getByRole('button', { name: /Save/i }).click()
    const download = await downloadPromise

    // Verify empty notebook is valid
    const downloadPath = await download.path()
    if (downloadPath) {
      const content = fs.readFileSync(downloadPath, 'utf-8')
      const notebook = JSON.parse(content)
      expect(notebook.cells).toHaveLength(0)
    }
  })

  test('undo/redo functionality', async ({ page }) => {
    // Create a cell
    await page.getByRole('button', { name: /Python/i }).first().click()
    await expect(page.locator('.cm-editor')).toHaveCount(1)

    // Add another cell
    await page.getByRole('button', { name: /Code/i }).last().click()
    await expect(page.locator('.cm-editor')).toHaveCount(2)

    // Undo (Ctrl+Z)
    await page.keyboard.press('Escape') // Exit editor
    await page.keyboard.press('Control+z')
    await expect(page.locator('.cm-editor')).toHaveCount(1)

    // Redo (Ctrl+Shift+Z)
    await page.keyboard.press('Control+Shift+z')
    await expect(page.locator('.cm-editor')).toHaveCount(2)
  })
})
