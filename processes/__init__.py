import os
pth = os.path.dirname(__file__)
import glob
pyfiles = glob.glob(os.path.join(pth,"*.py"))
__all__ = [ os.path.split(x[:-3])[-1] for x in pyfiles]
__all__.remove("__init__")

