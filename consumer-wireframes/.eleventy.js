module.exports = function(eleventyConfig) {
    eleventyConfig.addPassthroughCopy('assets');
    eleventyConfig.addPassthroughCopy('_data');
    eleventyConfig.addPassthroughCopy('downloads');
}
