from modules import ModuleRegistry
import os

directory = os.path.dirname(__file__)
package = os.path.basename( directory )
stagingRegistry = ModuleRegistry( 'Staging Method', directory, package )

