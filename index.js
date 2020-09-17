'use strict';

const Ajv = require('ajv');

const reISO = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}(?:\.\d*))(?:Z|(\+|-)([\d|:]*))?$/;

/**
 *
 * @param {*} key
 * @param {*} value
 */
function reviver(key, value) {
  if (typeof value === 'string' && reISO.test(value)) {
    return new Date(value);
  }
  return value;
}

/**
 *
 */
class DynormError extends Error {
  constructor(msg) {
    super(msg);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

/**
 * 
 */
class Dynorm {
  #models;
  #client;
  #schema;

  constructor() {
    this.#models = [];
  }

  get client() {
    return this.#client;
  }

  set client(value) {
    this.#client = value;
  }

  get schema() {
    return this.#schema;
  }

  set schema(schema) {
    this.#schema = schema;
  }

  /**
   *
   * @param {*} name
   * @param {*} schema
   */
  model(name, schema) {
    if (this.#models[name]) { return this.#models[name]; }
    const model = Model.compile(name, schema, this);
    this.#models[name] = model;
    return this.#models[name];
  }
};

const dynorm = new Dynorm();

/**
 *
 */
class Schema {
  #id;
  #methods;
  #statics;
  #virtuals;
  #key;

  #validate;
  #schema;
  #schemaName;

  constructor(id) {
    this.#id = id;
    this.#methods = {};
    this.#statics = {};
    this.#virtuals = {};
    this.#key = {};

    const defSchema = Object.assign({}, dynorm.schema);
    let schema = null;
    for (const schemaName in defSchema.definitions) {
      if (defSchema.definitions[schemaName].$id === this.#id) {
        schema = defSchema.definitions[schemaName];
        this.#schemaName = schemaName;
        break;
      }
    }

    for (const propName in schema.properties) {
      const prop = schema.properties[propName];
      const refSchemaName = prop.$ref;
      if (!refSchemaName || !prop.required) { continue; }
      defSchema.definitions[refSchemaName].required = prop.required;
      delete prop.required;

      // Delete self reference Schema
      const refSchema = defSchema.definitions[refSchemaName];
      for (const refPropName in refSchema.properties) {
        const refProp = refSchema.properties[refPropName];
        if (refProp.$ref === this.#schemaName) {
          delete refSchema.properties[refPropName];
        }
      }
    }

    const ajv = new Ajv();
    ajv.addSchema(defSchema);
    this.#validate = ajv.getSchema(id);
    this.#schema = this.#validate.schema;

    for (const k in this.#schema.properties) {
      const prop = this.#schema.properties[k];
      if (prop.hashKey) { this.#key.hashKey = k; };
      if (prop.rangeKey) { this.#key.rangeKey = k; };
    }
  }

  get validate() {
    return this.#validate;
  }

  get schema() {
    return this.#schema;
  }

  get methods() {
    return this.#methods;
  }

  get statics() {
    return this.#statics;
  }

  get virtuals() {
    return this.#virtuals;
  }

  get properties() {
    return this.#schema.properties;
  }

  get version() {
    for (const k in this.#schema.properties) {
      const prop = this.#schema.properties[k];
      if (prop.version) {
        return k;
      }
    }
  }

  get owner() {
    for (const k in this.#schema.properties) {
      const prop = this.#schema.properties[k];
      if (prop.owner) {
        return k;
      }
    }
  }

  get tableName() {
    return this.#schema.tableName;
  }

  get timestamps() {
    return this.#schema.timestamps;
  }

  get indexes() {
    return this.#schema.indexes;
  }

  get key() {
    return this.#key;
  }

  /**
   * Model to DynamoDB Item
   *
   * @param {*} model
   */
  toDynamo(model) {
    const item = {};
    for (const k in this.#schema.properties) {
      if (model[k] === undefined) { continue; };
      const prop = this.#schema.properties[k];
      if (prop.$ref) {
        for (const key in prop.join) {
          const v = prop.join[key];
          item[key] = model[k][v];
        }
      } else {
        if (model[k] instanceof Date) {
          item[k] = model[k].valueOf();
        } else {
          item[k] = model[k];
        }
      }
    }
    return item;
  }

  /**
   * DynamoDB Item to Model
   *
   * @param {*} item
   */
  parseDynamo(item) {
    const model = {};
    for (const k in this.#schema.properties) {
      const prop = this.#schema.properties[k];
      if (prop.$ref) {
        if (item[k]) {
          model[k] = item[k];
        } else {
          const key = Object.keys(prop.join)[0];
          if (!item[key]) { continue; }
          const fk = prop.join[key];
          model[k] = {};
          model[k][fk] = item[key];
        }
      } else {
        if (item[k] === undefined) { continue; };
        if (prop.format === 'date-time' || prop.format === 'date') {
          model[k] = new Date(item[k]);
        } else {
          model[k] = item[k];
        }
      }
    }
    return model;
  }

  /**
   *
   * @param {*} json
   */
  parseJSON(json) {
    return JSON.stringify(json, reviver);
  }
}

/**
 *
 */
class Model {
  _isNew;
  _data;

  constructor(data = {}, isNew = true) {
    this._isNew = isNew;
    this._data = data;
  }

  /**
   * Create a new Model whith a schema
   *
   * @param {*} name
   * @param {*} schema
   * @param {*} orm
   */
  static compile(name, schema, orm) {
    /**
     *  Model Definition
     */
    class NewModel extends Model {
      #schema;
      #orm;
      #orig;

      constructor(data, isNew) {
        super(data, isNew);
        this.#schema = schema;
        this.#orm = orm;
        this.#orig = Object.assign({}, data);

        for (const k in this.#schema.properties) {
          // Generate Get/Set funtions to schema properties
          Object.defineProperty(this, k, {
            get: function () { return Reflect.get(this._data, k); },
            set: function (value) { Reflect.set(this._data, k, value); }
          });

          // Initialize the default values of model
          const prop = this.#schema.properties[k];
          if (!this._data[k] && prop.default !== undefined) {
            if (prop.type === 'string' && prop.format === 'date-time') {
              if (prop.default === 'now') {
                this._data[k] = new Date();
              } else {
                this._data[k] = new Date(prop.default);
              }
            } else {
              this._data[k] = prop.default;
            }
          }
        }
      }

      /**
       *
       */
      toJSON() {
        return this._data;
      }

      /**
       *
       */
      async save() {
        // Validate relations
        for (const propName in this.#schema.properties) {
          if (!this._data[propName]) { continue; }
          const prop = this.#schema.properties[propName];
          if (!prop.$ref) { continue; }

          const obj = this._data[propName];
          if (obj instanceof Model) { continue; }

          const RefModel = this.#orm.model(prop.$ref);
          this._data[propName] = await RefModel.get(obj);

          if (!this._data[propName]) {
            throw new DynormError(`Relation ${propName} not exist`);
          }
        }

        // Validate model
        const json = JSON.parse(JSON.stringify(this));
        console.log('VALIDATION1', json);
        const valid = this.#schema.validate(json);
        if (!valid) {
          console.log('VALIDATION2', this.#schema.validate.errors);
          throw new DynormError(this.#schema.validate.errors);
        }

        // Validate unique index
        for (const k in this.#schema.indexes) {
          const index = this.#schema.indexes[k];
          if (!index.unique) { continue; }

          let hashKey = null;
          let hashVal = null;
          const prop = this.#schema.properties[index.hashKey];
          if (prop.$ref) {
            hashKey = Object.keys(prop.join)[0];
            const fk = prop.join[hashKey];
            hashVal = this._data[index.hashKey][fk];
          } else {
            hashKey = index.hashKey;
            hashVal = this._data[hashKey];
          }
          if (!hashVal) { throw new DynormError(`Unique index constraint ${k} hashKey ${hashKey} is empty`); }

          let rangeKey = null;
          let rangeVal = null;
          if (index.rangeKey) {
            const prop = this.#schema.properties[index.rangeKey];
            if (prop.$ref) {
              rangeKey = Object.keys(prop.join)[0];
              const fk = prop.join[rangeKey];
              rangeVal = this._data[index.rangeKey][fk];
            } else {
              rangeKey = index.rangeKey;
              rangeVal = this._data[rangeKey];
            }
            if (!rangeVal) { throw new DynormError(`Unique index constraint ${k} rangeKey ${rangeKey} is empty`); }
          }

          console.log(this.#schema);
          console.log(this.#schema.tableName);
          const params = {
            TableName: this.#schema.tableName,
            IndexName: k,
            ExpressionAttributeNames: {},
            ExpressionAttributeValues: {}
          };
          params.KeyConditionExpression = `#${hashKey} = :${hashKey}`;
          params.ExpressionAttributeNames[`#${hashKey}`] = hashKey;
          params.ExpressionAttributeValues[`:${hashKey}`] = hashVal;
          if (rangeKey) {
            params.KeyConditionExpression += ` AND #${rangeKey} = :${rangeKey}`;
            params.ExpressionAttributeNames[`#${rangeKey}`] = rangeKey;
            params.ExpressionAttributeValues[`:${rangeKey}`] = rangeVal;
          }

          if (!this._isNew) {
            const key = this.#schema.key;
            params.FilterExpression = `#${key.hashKey}Pk <> :${key.hashKey}Pk`;
            params.ExpressionAttributeNames[`#${key.hashKey}Pk`] = key.hashKey;
            params.ExpressionAttributeValues[`:${key.hashKey}Pk`] = this._data[key.hashKey];
            if (key.rangeKey) {
              params.FilterExpression += ` AND #${key.rangeKey}Pk <> :${key.rangeKey}Pk`;
              params.ExpressionAttributeNames[`#${key.rangeKey}Pk`] = key.rangeKey;
              params.ExpressionAttributeValues[`:${key.rangeKey}Pk`] = this._data[key.rangeKey];
            }
          }
          console.log('[save]Index', params);
          const { Count } = await this.#orm.client.query(params).promise();
          if (Count) { throw new DynormError(`Unique index constraint ${k}`); }
        }

        if (this.#schema.timestamps) {
          if (this._isNew) {
            this._data.createdAt = new Date();
            this._data.updatedAt = new Date();
          } else {
            this._data.createdAt = this.#orig.createdAt;
            this._data.updatedAt = this.#orig.updatedAt;
          }
        }

        const params = { TableName: this.#schema.tableName, Item: this.#schema.toDynamo(this._data) };

        // If schema has version porperty, set new version
        const ver = this.#schema.version;
        if (ver) {
          const prop = this.#schema.properties[ver];
          if (this.#orig && this.#orig[ver]) {
            params.ConditionExpression = (params.ConditionExpression) ? ' AND ' : '';
            if (!params.ExpressionAttributeNames) { params.ExpressionAttributeNames = {}; }
            if (!params.ExpressionAttributeValues) { params.ExpressionAttributeValues = {}; }
            params.ConditionExpression += `#${ver} = :${ver}`;
            params.ExpressionAttributeNames[`#${ver}`] = ver;
            if (prop.type === 'integer') {
              params.ExpressionAttributeValues[`:${ver}`] = this.#orig[ver];
              params.Item[ver]++;
            } else if (prop.type === 'string' && prop.format === 'date-time') {
              params.ExpressionAttributeValues[`:${ver}`] = this.#orig[ver].getTime();
              params.Item[ver] = Date.now();
            } else {
              throw new DynormError(`Version property type ${prop.type} not supported`);
            }
          } else if (params.Item[ver] === undefined) {
            if (prop.type === 'integer') {
              params.Item[ver] = 1;
            } else if (prop.type === 'string' && prop.format === 'date-time') {
              params.Item[ver] = Date.now();
            } else {
              throw new DynormError(`Version property type ${prop.type} not supported`);
            }
          }
        }

        if (this._isNew) {
          if (!params.ExpressionAttributeNames) { params.ExpressionAttributeNames = {}; }
          const key = this.#schema.key;
          params.ConditionExpression = (params.ConditionExpression) ? ' AND ' : '';
          params.ConditionExpression += `attribute_not_exists(#${key.hashKey})`;
          params.ExpressionAttributeNames[`#${key.hashKey}`] = key.hashKey;
          if (key.rangeKey) {
            params.ConditionExpression = (params.ConditionExpression) ? ' AND ' : '';
            params.ConditionExpression += `attribute_not_exists(#${key.rangeKey})`;
            params.ExpressionAttributeNames[`#${key.rangeKey}`] = key.rangeKey;
          }
        }

        console.log('[save]', params);
        await this.#orm.client.put(params).promise();
      }

      /**
       *
       */
      async del() {
        const key = Object.values(this.#schema.key).reduce((a, c) => { a[c] = this[c]; return a; }, {});
        const params = { TableName: this.#schema.tableName, Key: key };
        await this.#orm.client.delete(params).promise();
      }

      /**
       *
       * @param {*} client
       * @param {*} fields
       * @param {*} items
       */
      static async populate(client, fields = [], items = []) {
        if (!fields.length) { return; }
        if (!items.length) { return; }

        const tableKeys = {};
        const keys = {};

        for (const field of fields) {
          const prop = schema.properties[field];
          if (!prop) { continue; }
          for (const item of items) {
            const key = Object.keys(prop.join)[0];
            const val = item[key];
            if (!val) { continue; }

            const fk = prop.join[key];
            const RefModel = orm.model(prop.$ref);
            const tableName = RefModel.schema.tableName;
            if (!tableKeys[tableName]) { tableKeys[tableName] = []; }

            // Generate a index table key to avoid duplicate keys
            if (!keys[`${tableName}_${val}`]) {
              const itemKey = {};
              itemKey[fk] = item[key];
              tableKeys[tableName].push(itemKey);
              keys[`${tableName}_${val}`] = true;
            }
          }
        }

        const tableItems = await Model.batchGetKeys(client, tableKeys);

        for (const field of fields) {
          const prop = schema.properties[field];
          if (!prop) { continue; }
          for (const item of items) {
            const key = Object.keys(prop.join)[0];
            if (!item[key]) { continue; }

            const fk = prop.join[key];
            const RefModel = orm.model(prop.$ref);
            const tableName = RefModel.schema.tableName;

            const tableItem = tableItems[tableName].find(i => i[fk] === item[key]);
            const refModel = RefModel.schema.parseDynamo(tableItem);
            item[field] = new RefModel(refModel, false);
          }
        }
      };

      /**
       *
       * @param {*} key
       * @param {*} update
       */
      static async update(key, update) {
        const params = {
          TableName: schema.tableName,
          Key: key,
          ExpressionAttributeNames: {},
          ExpressionAttributeValues: {},
          ReturnValues: 'ALL_NEW'
        };
        const type = ['$SET', '$ADD', '$DELETE'].find(t => update[t]);
        if (type === '$ADD') {
          params.UpdateExpression = 'ADD ';
        } else if (type === '$DELETE') {
          params.UpdateExpression = 'DELETE ';
        } else {
          params.UpdateExpression = 'SET ';
        }
        if (schema.version) {
          const k = schema.version;
          // if (Object.keys(params.ExpressionAttributeNames).length) params.UpdateExpression += ', ';
          params.UpdateExpression += `#${k} = :${k}`;
          params.ExpressionAttributeNames[`#${k}`] = k;
          params.ExpressionAttributeValues[`:${k}`] = Date.now();
          if (update[type][k]) {
            params.ConditionExpression = `#${k} = :${k}Old`;
            params.ExpressionAttributeValues[`:${k}Old`] = update[type][k];
          }
          delete update[type][k];
        }
        Object.keys(key).forEach(k => delete update[type][k]);
        Object.keys(update[type]).forEach(k => {
          if (Object.keys(params.ExpressionAttributeNames).length) params.UpdateExpression += ', ';
          params.UpdateExpression += `#${k} = :${k}`;
          params.ExpressionAttributeNames[`#${k}`] = k;
          params.ExpressionAttributeValues[`:${k}`] = update[type][k];
        });
        console.log(params);
        const data = await orm.client.update(params).promise();
        return data.Attributes;
      }

      /**
       *
       * @param {*} key
       * @param {*} fields
       */
      static async get(key, fields = []) {
        key = Object.values(schema.key).reduce((a, c) => { a[c] = key[c]; return a; }, {});
        const params = { TableName: schema.tableName, Key: key };
        const { Item } = await orm.client.get(params).promise();
        if (!Item) { return null; };

        await NewModel.populate(orm.client, fields, [Item]);
        return new NewModel(schema.parseDynamo(Item), false);
      }

      /**
       *
       * @param {*} params
       * @param {*} fields
       */
      static async find(params = {}, fields = []) {
        params.TableName = schema.tableName;
        const result = await Model.find(orm.client, params);
        await NewModel.populate(orm.client, fields, result.Items);
        result.Items = result.Items.map(item => new NewModel(schema.parseDynamo(item), false));
        return result;
      }

      /**
       *
       */
      static get schema() {
        return schema;
      }

      /**
       *
       */
      static get orm() {
        return orm;
      }

      /**
       *
       * @param {*} json
       */
      static fromJSON(json) {
        return new NewModel(schema.parseJSON(json));
      }
    }

    Object.defineProperty(NewModel, 'name', {
      value: name
    });

    return NewModel;
  }

  /**
   *
   * @param {*} client
   * @param {*} params
   */
  static async batchWrite(client, params) {
    const data = await client.batchWrite(params).promise();
    if (Object.keys(data.UnprocessedItems).length) {
      params.RequestItems = data.UnprocessedItems;
      await Model.batchWrite(client, params);
    }
  }

  /**
   *
   * @param {*} client
   * @param {*} tableItems
   */
  static async batchWritePuts(client, tableItems) {
    if (!Object.keys(tableItems).length) { return {}; };
    const items = [];
    Object.keys(tableItems).forEach(tn => tableItems[tn].forEach(i => items.push({ tn: tn, i: i })));
    const len = items.length / 25;
    for (let x = 0, i = 0; x < len; i += 25, x++) {
      const params = { RequestItems: {} };
      items.slice(i, i + 25).map(ti => {
        if (!params.RequestItems[ti.tn]) params.RequestItems[ti.tn] = [];
        params.RequestItems[ti.tn].push({ PutRequest: { Item: ti.i } });
      });
      await Model.batchWrite(client, params);
    }
  };

  /**
   *
   * @param {*} client
   * @param {*} tableKeys
   */
  static async batchWriteDeletes(client, tableKeys) {
    if (!Object.keys(tableKeys).length) { return {}; };
    const items = [];
    Object.keys(tableKeys).forEach(tn => tableKeys[tn].forEach(i => items.push({ tn: tn, i: i })));
    const len = items.length / 25;
    for (let x = 0, i = 0; x < len; i += 25, x++) {
      const params = { RequestItems: {} };
      items.slice(i, i + 25).map(ti => {
        if (!params.RequestItems[ti.tn]) params.RequestItems[ti.tn] = [];
        params.RequestItems[ti.tn].push({ DeleteRequest: { Key: ti.i } });
      });
      await Model.batchWrite(client, params);
    }
  };

  /**
   *
   * @param {*} client
   * @param {*} params
   */
  static async batchGet(client, params) {
    const data = await client.batchGet(params).promise();
    let obj = data.Responses;
    if (Object.keys(data.UnprocessedKeys).length) {
      params.RequestItems = data.UnprocessedKeys;
      const res = await Model.batchGet(client, params);
      obj = Object.keys(res).reduce((a, c) => { a[c] = (a[c]) ? a[c].concat(res[c]) : res[c]; return a; }, obj);
    }
    return obj;
  };

  /**
   *
   * @param {*} client
   * @param {*} tableKeys
   */
  static async batchGetKeys(client, tableKeys) {
    if (!Object.keys(tableKeys).length) return {};
    let obj = Object.keys(tableKeys).reduce((a, c) => { a[c] = []; return a; }, {});
    const keys = [];
    Object.keys(tableKeys).forEach(t => tableKeys[t].forEach(k => keys.push({ t: t, k: k })));
    const len = keys.length / 100;
    for (let x = 0, i = 0; x < len; i += 100, x++) {
      const params = { RequestItems: {} };
      keys.slice(i, i + 100).forEach(item => {
        if (!params.RequestItems[item.t]) params.RequestItems[item.t] = { Keys: [] };
        params.RequestItems[item.t].Keys.push(item.k);
      });
      const res = await Model.batchGet(client, params);
      obj = Object.keys(res).reduce((a, c) => { a[c] = (a[c]) ? a[c].concat(res[c]) : res[c]; return a; }, obj);
    }
    return obj;
  };

  /**
   * Make a query o scan recursively and return un object with items o reduce data
   *
   * @param {*} client DocumentClient
   * @param {*} params Query prams
   * @param {*} opts filer, map or reduce functions
   * @param {*} acc accumulated object to return
   */
  static async find(client, params, opts = {}, acc = { Items: [], ScannedCount: 0 }) {
    if (opts.map && opts.reduce) { throw new Error('Only map or reduce is required'); }
    if (opts.reduce && !opts.initialValue) { throw new Error('Reduce initialValue is required'); }
    if (opts.reduce && !acc.Accumulator) { acc.Accumulator = opts.initialValue; };

    const data = (params.KeyConditionExpression) ? await client.query(params).promise() : await client.scan(params).promise();

    acc.ScannedCount += data.ScannedCount;
    if (opts.filter) { data.Items = data.Items.filter(opts.filter); }
    if (opts.map) { data.Items = await Promise.all(data.Items.map(opts.map)); };
    if (opts.reduce) {
      acc.Accumulator = await data.Items.reduce(opts.reduce, acc.Accumulator);
    } else {
      acc.Items = acc.Items.concat(data.Items);
      acc.Count = acc.Items.length;
    };

    if (params.Limit) { params.Limit -= data.Items.length; }
    if (data.LastEvaluatedKey) {
      if (params.Limit === undefined || params.Limit > 0) {
        params.ExclusiveStartKey = data.LastEvaluatedKey;
        await Model.find(client, params, opts, acc);
      } else {
        acc.LastEvaluatedKey = data.LastEvaluatedKey;
      }
    }

    return acc;
  }
}

module.exports.dynorm = dynorm;
module.exports.Model = Model;
module.exports.Schema = Schema;
