export const deepmerge = (target, ...source) => {
  const isObject = (obj) => {
    return obj && typeof obj === "object" && !Array.isArray(obj);
  };

  const merge = (target, source) => {
    for (const key in source) {
      if (isObject(source[key]) && isObject(target[key])) {
        target[key] = merge(target[key], source[key]);
      } else {
        target[key] = source[key];
      }
    }
    return target;
  };

  return source.reduce(merge, target);
};
