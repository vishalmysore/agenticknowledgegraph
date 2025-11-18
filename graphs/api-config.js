<!-- Configuration for API Base URL - injected at runtime -->
<script>
    // Get API base URL from environment variable or use current location
    window.API_BASE_URL = (function() {
        // Try to get from meta tag first (if server injects it)
        const metaTag = document.querySelector('meta[name="api-base-url"]');
        if (metaTag) {
            return metaTag.getAttribute('content');
        }

        // Fall back to environment variable if available
        if (typeof process !== 'undefined' && process.env.REACT_APP_API_URL) {
            return process.env.REACT_APP_API_URL;
        }

        // Default to current host
        return window.location.protocol + '//' + window.location.host;
    })();

    console.log('API Base URL configured as:', window.API_BASE_URL);
</script>

