(function registerAtlasCapabilities(globalObj) {
    const API = {
        defaultCapabilities() {
            return {
                loaded: false,
                chat_enabled: false,
                ai_enabled: false,
                api_version: null,
                year_policy: null
            };
        },

        normalize(payload) {
            return {
                loaded: true,
                chat_enabled: Boolean(payload && payload.chat_enabled),
                ai_enabled: Boolean(payload && payload.ai_enabled),
                api_version: payload && payload.api_version ? payload.api_version : null,
                year_policy: payload && payload.year_policy ? payload.year_policy : null
            };
        },

        isChatEnabled(capabilities) {
            return Boolean(capabilities && capabilities.chat_enabled);
        }
    };

    globalObj.AtlasCapabilities = API;
})(window);
