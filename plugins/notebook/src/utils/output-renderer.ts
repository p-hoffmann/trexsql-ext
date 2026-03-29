import type { MimeBundle } from '@/types/notebook'

export const MIME_PRIORITY = [
  'text/html',
  'image/svg+xml',
  'image/png',
  'image/jpeg',
  'text/markdown',
  'text/plain',
] as const

export function getBestMimeType(data: MimeBundle): string {
  for (const mimeType of MIME_PRIORITY) {
    if (data[mimeType] !== undefined) {
      return mimeType
    }
  }
  return 'text/plain'
}

export function isImageMimeType(mimeType: string): boolean {
  return mimeType.startsWith('image/')
}

export function isHtmlMimeType(mimeType: string): boolean {
  return mimeType === 'text/html'
}

export function toImageDataUrl(base64: string, mimeType: string): string {
  if (base64.startsWith('data:')) {
    return base64
  }
  return `data:${mimeType};base64,${base64}`
}

export function estimateOutputSize(data: MimeBundle): number {
  let size = 0
  for (const value of Object.values(data)) {
    if (typeof value === 'string') {
      size += value.length
    } else if (value !== null && value !== undefined) {
      size += JSON.stringify(value).length
    }
  }
  return size
}

export function truncateText(text: string, maxLength: number): { text: string; truncated: boolean } {
  if (text.length <= maxLength) {
    return { text, truncated: false }
  }
  return {
    text: text.slice(0, maxLength) + '\n... (output truncated)',
    truncated: true,
  }
}

export function isOutputOversized(data: MimeBundle, maxBytes: number = 500 * 1024): boolean {
  return estimateOutputSize(data) > maxBytes
}

export function stripAnsi(text: string): string {
  // eslint-disable-next-line no-control-regex
  return text.replace(/\x1b\[[0-9;]*[a-zA-Z]/g, '')
}

/** Converts common ANSI color codes to HTML spans */
export function ansiToHtml(text: string): string {
  const colorMap: Record<number, string> = {
    30: 'ansi-black',
    31: 'ansi-red',
    32: 'ansi-green',
    33: 'ansi-yellow',
    34: 'ansi-blue',
    35: 'ansi-magenta',
    36: 'ansi-cyan',
    37: 'ansi-white',
    90: 'ansi-bright-black',
    91: 'ansi-bright-red',
    92: 'ansi-bright-green',
    93: 'ansi-bright-yellow',
    94: 'ansi-bright-blue',
    95: 'ansi-bright-magenta',
    96: 'ansi-bright-cyan',
    97: 'ansi-bright-white',
  }

  let result = ''
  let currentClass = ''
  let i = 0

  while (i < text.length) {
    if (text[i] === '\x1b' && text[i + 1] === '[') {
      let j = i + 2
      while (j < text.length && !/[a-zA-Z]/.test(text[j])) {
        j++
      }

      if (j < text.length && text[j] === 'm') {
        const codes = text.slice(i + 2, j).split(';').map(Number)

        if (currentClass) {
          result += '</span>'
          currentClass = ''
        }

        if (codes.includes(0)) {
          currentClass = ''
        } else {
          for (const code of codes) {
            if (colorMap[code]) {
              currentClass = colorMap[code]
              result += `<span class="${currentClass}">`
              break
            }
          }
        }

        i = j + 1
        continue
      }
    }

    if (text[i] === '<') {
      result += '&lt;'
    } else if (text[i] === '>') {
      result += '&gt;'
    } else if (text[i] === '&') {
      result += '&amp;'
    } else {
      result += text[i]
    }
    i++
  }

  if (currentClass) {
    result += '</span>'
  }

  return result
}
