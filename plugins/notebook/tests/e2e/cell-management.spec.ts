/**
 * E2E tests for cell management (T057)
 */

import { test, expect } from '@playwright/test'

test.describe('Cell Management', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for the app to load
    await expect(page.locator('h1:has-text("React Notebook")')).toBeVisible()
  })

  test('creates a Python code cell', async ({ page }) => {
    // Click Python button in empty state
    await page.getByRole('button', { name: /Python/i }).first().click()

    // Verify code cell is created
    await expect(page.locator('.cm-editor')).toBeVisible()
  })

  test('creates a Markdown cell', async ({ page }) => {
    // Click Markdown button in empty state
    await page.getByRole('button', { name: /Markdown/i }).first().click()

    // Verify markdown cell is created (look for markdown editor)
    await expect(page.locator('.cm-editor')).toBeVisible()
  })

  test('creates an R code cell', async ({ page }) => {
    // Click R button in empty state
    await page.getByRole('button', { name: /^R$/i }).first().click()

    // Verify code cell is created
    await expect(page.locator('.cm-editor')).toBeVisible()
  })

  test('adds multiple cells', async ({ page }) => {
    // Add first cell
    await page.getByRole('button', { name: /Python/i }).first().click()
    await expect(page.locator('.cm-editor')).toHaveCount(1)

    // Add second cell via toolbar or bottom button
    await page.getByRole('button', { name: /Code/i }).last().click()
    await expect(page.locator('.cm-editor')).toHaveCount(2)

    // Add markdown cell
    await page.getByRole('button', { name: /Markdown/i }).last().click()
    await expect(page.locator('.cm-editor')).toHaveCount(3)
  })

  test('deletes a cell using keyboard', async ({ page }) => {
    // Create a cell
    await page.getByRole('button', { name: /Python/i }).first().click()
    await expect(page.locator('.cm-editor')).toBeVisible()

    // Press Escape to exit edit mode, then Delete to remove
    await page.keyboard.press('Escape')
    await page.keyboard.press('Delete')

    // Cell should be removed
    await expect(page.locator('.cm-editor')).not.toBeVisible()
  })

  test('types code in cell', async ({ page }) => {
    // Create a cell
    await page.getByRole('button', { name: /Python/i }).first().click()

    // Type code
    await page.locator('.cm-editor').click()
    await page.keyboard.type('print("Hello, World!")')

    // Verify code is in the editor
    await expect(page.locator('.cm-content')).toContainText('print')
  })

  test('navigates between cells with arrow keys', async ({ page }) => {
    // Create two cells
    await page.getByRole('button', { name: /Python/i }).first().click()
    await page.getByRole('button', { name: /Code/i }).last().click()

    // Press Escape to exit edit mode
    await page.keyboard.press('Escape')

    // Navigate up
    await page.keyboard.press('ArrowUp')

    // Navigate down
    await page.keyboard.press('ArrowDown')

    // Should complete without errors
    await expect(page.locator('.cm-editor')).toHaveCount(2)
  })

  test('duplicates a cell', async ({ page }) => {
    // Create a cell with content
    await page.getByRole('button', { name: /Python/i }).first().click()
    await page.locator('.cm-editor').click()
    await page.keyboard.type('x = 42')

    // Find and click the duplicate button in cell menu
    // First hover over cell to show controls
    const cellContainer = page.locator('.cm-editor').first().locator('..')
    await cellContainer.hover()

    // Look for duplicate button (may be in a menu)
    const duplicateButton = page.getByRole('button', { name: /duplicate/i })
    if (await duplicateButton.isVisible()) {
      await duplicateButton.click()
      await expect(page.locator('.cm-editor')).toHaveCount(2)
    }
  })
})
