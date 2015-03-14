/**
 * JSON CSS
 *
 * @author    Chris Dary <umbrae@gmail.com>
 * @copyright Copyright (c) 2008 {@link http://arc90.com Arc90 Inc.}
 * @license   http://www.opensource.org/licenses/bsd-license.php
 */
jQuery.jsoncss = function(path) {
    /**
     * @var object Contains an associative array of all styles that have been applied so far.
     *             This is used when using @inherit to inherit class styles.
    **/
    var allStyles = {};

    /**
     * @var object Contains all of the @variables defined in the JSON CSS file. Gets looped over
     *             and applied to every css property.
    **/
    var styleVariables = {};

    /**
     * Recursively apply JSON CSS styles to a selector.
     *
     * @param selector string A jquery capable selector, as defined here: http://docs.jquery.com/Selectors
     * @param styles   object A collection of CSS styles to be applied to this selector. If the value
     *                        of one of the attributes is an object, it's assumed to be a subselector,
     *                        and applyStyles will be recursively called with the key as the subselector.
     * @return void
    **/
    var applyStyles = function(selector, styles) {
        /**
         * @var object Hold our browser specific styles for this selector in an associative
         *             array so that we can apply them with precedence afterward
        **/
        var browserStyles = {};

        /**
         * look for attributes that require special handling.
         * Currently: @variables, @inherit, @browser, and subselectors
        **/
        for(var styleAttrib in styles)
        {
            var styleValue = styles[styleAttrib];

            if(styleAttrib[0] == '@')
            {
                if(styleAttrib == "@variables")
                {
                    /**
                     * Add any @variable definitions to our global styleVariables associative array
                    **/
                    jQuery.extend(styleVariables, styleValue);
                }
                else if(styleAttrib == "@inherit")
                {
                    /**
                     * Inherit any selectors defined in @inherit.
                     * Note that there can be more than one, split by commas.
                    **/
                    inheritedStyles = styleValue.split(',');
                    for(inheritedStyleCounter in inheritedStyles)
                    {
                        inheritedStyleSelector = inheritedStyles[inheritedStyleCounter];

                        if(typeof allStyles[inheritedStyleSelector] != 'undefined')
                        {
                            var newStyle = allStyles[inheritedStyleSelector];
                            jQuery.fn.extend(newStyle, styles);
                            styles = newStyle;                      
                        }
                        else
                        {
                            alert('JSON CSS Error: Attempting to inherit from a selector that does not yet exist: ' + inheritedStyleSelector);
                        }
                    }
                }
                else if(styleAttrib.indexOf("@browser") === 0)
                {
                    /**
                     * If the value of @browser[<BROWSER>] matches ours, and optionally our version
                     * (like @browser[BROWSER-VERSION]) use the CSS rules defined within this object.
                    **/
                    var browserInfo = styleAttrib.replace(/@browser\[([^\]]+)\]/, '$1').split('-');
                    var browserMatch = browserInfo[0];
                    var browserVersion = browserInfo[1];

                    if( jQuery.browser[browserMatch] &&
                        (
                            !browserVersion ||
                            jQuery.browser.version.indexOf(browserVersion) === 0
                        )
                    )
                    {
                        browserStyles[selector] = styleValue;
                    }
                }

                /**
                 * Remove this from styles because it's not actually a style. Don't let jQuery try to apply it.
                **/
                delete styles[styleAttrib];
            }
            else if(typeof styleValue == "object")
            {
                /**
                 * We have a subselector. To cascade, recurse into it with it as the argument.
                **/
                if(selector.indexOf(',') != -1)
                {
                    alert("JSON CSS Error: Cannot cascade beneath a grouped selector. Action is undefined. Current selector: " + selector);
                    continue;
                }

                /**
                 * If the subselector starts with : or ., don't add a space because it's a class/pseudoclass selector
                **/
                var subSelector = selector + ((styleAttrib[0] == ':' || styleAttrib[0] == '.') ? '' : ' ') + styleAttrib;
                applyStyles(subSelector, styleValue);

                delete styles[styleAttrib];
            }
            else if(styleValue.indexOf('@{') != -1)
            {
                /**
                 * There are @variables in this CSS Rule. Loop over known variables and replace.
                **/
                for(var styleVariable in styleVariables)
                {
                    styles[styleAttrib] = styles[styleAttrib].replace('@{' + styleVariable + '}', styleVariables[styleVariable]);
                }
            }
        }

        /**
         * Add the styles object for this selector into allStyles for future possible inheritance.
        **/
        allStyles[selector] = styles;

        /**
         * All preprocessing is finished. Apply the cleaned CSS object to our selector.
        **/
        jQuery(selector).css(styles);

        /**
         * Now apply any browser specific styling we encountered. Done last for precedence.
        **/
        for(var browserStyleSelector in browserStyles)
        {
            jQuery(browserStyleSelector).css(browserStyles[browserStyleSelector]);
        }
    }       

    /**
     * Hide the content until rendered - to avoid the new 'flash of unstyled content'
    **/
    jQuery('body').hide();

    /**
     * Get our JSON CSS and recursively apply the styles within it.
    **/
    jQuery.getJSON(path, function(style) {
            applyStyles('', style);
            
            /**
             * When finished, display the content.
            **/
            jQuery('body').show();
        }
    );
}
