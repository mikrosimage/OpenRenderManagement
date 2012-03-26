
import os


modules = [__import__(module[:-3]) for module in os.listdir(os.path.dirname(__file__)) if module.endswith(".py") and module.startswith("test_")]

for module in modules:
    print module
    classes = [classe for classe in dir(module) if classe.startswith("Test")]
    for classe in classes:        
        globals()[classe] = getattr(module, classe)
