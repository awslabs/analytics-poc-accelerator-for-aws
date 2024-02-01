/** @type {import('tailwindcss').Config} */
module.exports = {
    content: [
      "./**/*.html",
      "./static/src/**/*.js",
      "./node_modules/flowbite/**/*.js"
    ],
    theme: {
      extend: {},
    },
    plugins: [
      require("flowbite/plugin")
    ],
  }
  
  