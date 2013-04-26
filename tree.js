var util = require('util');

// All insertions are red.
// On insertion, when the parent and uncle are red, flip the parent to black
// When the uncle is black...
// If red is on the right, rotate it to the left.
// Then do a final rotation to fix it.
//
// When pointing to the left, it can rotate to the right.
// When pointing to the right, it can rotate to the left.

var RedBlackTree = module.exports = function() {
  this.root = null;
  this.length = 0;
};

var color = { red: true, black: false };

RedBlackTree.prototype._defaultComparator = function(a, b) {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
};

RedBlackTree.prototype.find = function(key, comparator) {
  var node = this.root;

  if (!comparator) {
    comparator = this._defaultComparator;
  }

  while (node) {
    var comparison = comparator(key, node.key);
    if (comparison === 0) {
      return node;
    }

    if (comparison === -1) {
      node = node.left;
    } else {
      node = node.right;
    }
  }

  return node;
};

RedBlackTree.prototype.insert = function(key, value, comparator) {
  this.length++;
  if (!comparator) {
    comparator = this._defaultComparator;
  }

  if (!this.root) {
    this.root = new ValueNode();
    this.root.color = color.black;
    this.root.key = key;
    this.root.value = value;
  } else {
    var node = new ValueNode();
    node.key = key;
    node.value = value;
    node.color = color.red;
    this.setParent(this.root, node, comparator);
    this.rebalance(node);
  }
};

RedBlackTree.prototype.setParent = function(start, node, comparator) {
  var comparison = comparator(node.key, start.key);
  if (comparison === -1) {
    if (!start.left || (start.left.key === undefined || start.left.key === null)) {
      start.left = node;
      node.parent = start;
    } else {
      this.setParent(start.left, node, comparator);
    }
  } else if (comparison === 1) {
    if (!start.right || (start.right.key === undefined || start.right.key === null)) {
      start.right = node;
      node.parent = start;
    } else {
      this.setParent(start.right, node, comparator);
    }
  } else if (comparison === 0) {
    start.value = node.value;
  }
};

RedBlackTree.prototype.rebalance = function(node) {
  if (!node.parent) {
    node.color = color.black;
    return;
  }

  if (node.parent.color === color.black) {
    return;
  }

  if (node.uncle.color === color.red) {
    node.parent.color = color.black;
    node.uncle.color = color.black;
    node.grandparent.color = color.red;
    this.rebalance(node.grandparent);
  } else {
    if (node.isRight() && node.parent.isLeft()) {
      this.rotateLeft(node.parent);
      node = node.left;
    } else if (node.isLeft() && node.parent.isRight()) {
      this.rotateRight(node.parent);
      node = node.right;
    }

    node.parent.color = color.black;
    node.grandparent.color = color.red;
    if (node.isLeft()/* && node.parent.isRight()*/) {
      this.rotateRight(node.grandparent);
    } else {
      this.rotateLeft(node.grandparent);
    }
  }
};

RedBlackTree.prototype.remove = function(key) {
  this.length--;
  var n = this.find(key);
  if (!n) {
    return;
  }

  if (n.left.key && n.right.key) {
    var pred = Math.max(n.left);
    n.key = pred.key;
    n.value = pred.value;
    n = pred;
  }

  var child = (!n.right || !n.right.key) ? n.left : n.right;

  if (n.color === color.black) {
    n.color = child.color;
    this._delete(child);
  }

  this.replaceNode(n, child);

  if (n.color === color.red) {
    this.root.color = color.black;
  }
  /*if (!n.parent && child) {
    child.color = color.black;
  }*/
};

RedBlackTree.prototype._delete = function(n) {
  if (!n.parent) {
    return;
  } else {
    if (n.sibling && n.sibling.color === color.red) {
      n.parent.color = red;
      n.sibling.color = black;
      if (n.isLeft()) {
        this.rotateLeft(n);
      } else {
        this.rotateRight(n);
      }
    }

    if (n.parent.color === color.black && (!n.sibling ||
        n.sibling.color === color.black) &&
        (!n.sibling.left || n.sibling.left.color === color.black) &&
        (!n.sibling.right || n.sibling.right.color === color.black)) {
          n.sibling.color = color.red;
          this._delete(n.parent);
    } else {
      if (n.parent.color === color.red &&
          n.sibling.color === color.black &&
          n.sibling.left.color === color.black &&
          n.sibling.right.color === color.black) {
            n.sibling.color = color.red;
            n.parent.color = color.black;
      } else {
        if (n.isLeft() &&
            n.sibling.color === color.black &&
            n.sibling.left.color === color.red &&
            n.sibling.right.color === color.black) {
              n.sibling.color = color.red;
              n.sibling.left.color = color.black;
              this.rotateRight(n.sibling);
        } else if (n.isRight() &&
            n.sibling.color === color.black &&
            n.sibling.right.color === color.red &&
            n.sibling.left.color === color.black) {
              n.sibling.color = color.red;
              n.sibling.right.color = color.black;
              this.rotateLeft(n.sibling);
        }

        n.sibling.color = n.parent.color;
        n.parent.color = color.black;
        if (n.isLeft()) {
          n.sibling.right.color = color.black;
          this.rotateLeft(n.parent);
        } else {
          n.sibling.left.color = color.black;
          this.rotateRight(n.parent);
        }
      }
    }
  }
};

RedBlackTree.prototype.max = function(node) {
  while (node.right.key) {
    node = node.right;
  }

  return node;
};

RedBlackTree.prototype.rotateLeft = function(node) {
  var right = node.right;
  this.replaceNode(node, right);
  node.right = right.left;
  if (right.left) {
    right.left.parent = node;
  }
  right.left = node;
  node.parent = right;
};

RedBlackTree.prototype.rotateRight = function(node) {
  var left = node.left;
  this.replaceNode(node, left);
  node.left = left.right;
  if (left.right) {
    left.right.parent = node;
  }
  left.right = node;
  node.parent = left;
};

RedBlackTree.prototype.replaceNode = function(o, n) {
  if (!o.parent) {
    this.root = n;
  } else {
    if (o.isLeft()) {
      o.parent.left = n;
    } else {
      o.parent.right = n;
    }
  }

  if (n) {
    n.parent = o.parent;
  }
};

var ValueNode = function() {
  this.color = color.black;
  this.key = null;
  this.value = null;
  this.parent = null;
  this.left = new Sentinel();
  this.right = new Sentinel();
};

var Sentinel = function() {
  this.color = color.black;
  this.key = null;
  this.value = null;
  this.parent = null;
};

ValueNode.prototype.__defineGetter__('grandparent', function() {
  return this.parent.parent;
});

ValueNode.prototype.__defineGetter__('uncle', function() {
  return this.parent.sibling;
});

ValueNode.prototype.__defineGetter__('sibling', function() {
  var dir = this.isLeft() ? 'right' : 'left';
  return this.parent[dir];
});

ValueNode.prototype.isLeft = function() {
  if (!this.parent.left) {
    return false;
  }
  return this.key === this.parent.left.key;
};

ValueNode.prototype.isRight = function() {
  if (!this.parent.right) {
    return false;
  }
  return this.key === this.parent.right.key;
};
