#include "TechnologyLoader.hpp"
#include <stdexcept>
#ifdef _WIN32
    #include <windows.h>
#else
    #include <dlfcn.h>
#endif

bool TechnologyLoader::load_technology(const std::string& lib_name, std::shared_ptr<Logger> logger) {
//todo: error handling?
#ifdef _WIN32
    HMODULE lib_handle = LoadLibraryA(lib_name.c_str());
    if (!lib_handle) {
        logger->log_error("[Technology Loader] Failed to load DLL: " + lib_name);
        return false;
    }

    using RegisterFunc = void(*)(std::shared_ptr<Logger>);
    RegisterFunc register_tech = reinterpret_cast<RegisterFunc>(
        GetProcAddress(lib_handle, "register_technology")
    );
    if (!register_tech) {
        logger->log_error("[Technology Loader] Failed to find symbol 'register_technology' in: " + lib_name);
        return false;
    }
#else
    void* handle = dlopen(lib_name.c_str(), RTLD_LAZY);
    if (!handle) {
        logger->log_error("[Technology Loader] dlopen failed: " + std::string(dlerror()));
    }

    using RegFunc = void(*)(std::shared_ptr<Logger>);
    RegFunc register_tech = (RegFunc)dlsym(handle, "register_technology");
    if (!register_tech) {
        logger->log_error("[Technology Loader] dlsym failed: " + std::string(dlerror()));
    }

#endif
    register_tech(logger);
    logger->log_info("[Technology Loader] Successfully loaded and registered: " + lib_name);
    return true;
}
