from modules.registry import ModuleRegistry
import os

directory = os.path.dirname(__file__)
package = os.path.basename( directory )
kernelRegistry = ModuleRegistry( 'Kernels', directory, package )




