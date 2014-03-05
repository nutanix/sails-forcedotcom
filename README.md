![image_squidhome@2x.png](http://i.imgur.com/RIvu9.png)

# ForceAdapter

Waterline adapter for Force.com.

## Installation

Install from NPM.

```bash
$ npm install sails-force --save
```

## Sails Configuration

Add the salesforce config to the `config/adapters.js` file.

### Using with Sails v0.9.x

```javascript
module.exports.adapters = {
  salesforce: {
    module: 'sails-force',
    connectionParams: {
      loginUrl: 'http://test.salesforce.com'
    },
    username: '{salesforce username}',
    password: '{salesforce password}' + '{security token}'
  }
}
```
