import type { KernelPlugin, KernelFactory, KernelRegistry } from './types'

class KernelRegistryImpl implements KernelRegistry {
  private factories = new Map<string, KernelFactory>()
  private instances = new Map<string, KernelPlugin>()

  register(id: string, factory: KernelFactory): void {
    this.factories.set(id, factory)
  }

  get(id: string): KernelPlugin | undefined {
    const cached = this.instances.get(id)
    if (cached) {
      return cached
    }

    const factory = this.factories.get(id)
    if (!factory) {
      return undefined
    }

    const instance = factory()
    this.instances.set(id, instance)
    return instance
  }

  list(): string[] {
    return Array.from(this.factories.keys())
  }

  getByLanguage(language: 'python' | 'r'): string[] {
    const result: string[] = []
    for (const [id] of this.factories) {
      const instance = this.get(id)
      if (instance && instance.languages.includes(language)) {
        result.push(id)
      }
    }
    return result
  }

  clearInstances(): void {
    this.instances.clear()
  }
}

export const kernelRegistry = new KernelRegistryImpl()
