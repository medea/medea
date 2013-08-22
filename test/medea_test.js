var assert = require('assert');
var Medea = require('../');

describe('Medea', function() {
  it('has a max file size', function() {
    var medea = new Medea();
    assert(medea.maxFileSize > 0);
  });
});
