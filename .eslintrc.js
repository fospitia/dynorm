module.exports = {
  "extends": "standard",
  "parser": "babel-eslint",
  "parserOptions": {
    "ecmaVersion": 11,
    "ecmaFeatures": {
      "impliedStrict": true
    }
  },
  "rules": {
    "semi": [2, "always"],
    "space-before-function-paren": ["error", {
      "anonymous": "always",
      "named": "never",
      "asyncArrow": "always"
    }]
  }
}
