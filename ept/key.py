from ept.boundingboxes import BoundingBox3D


class Key:
    def __init__(self, bounds):
        self.bounds = bounds
        self.x = 0
        self.y = 0
        self.z = 0
        self.d = 0

    def id_at(self, i):
        if i == 0:
            return self.x
        elif i == 1:
            return self.y
        elif i == 2:
            return self.z
        else:
            raise ValueError("id_at index not in range(0, 3)")

    def set_id_at(self, i, value):
        if i == 0:
            self.x = value
        elif i == 1:
            self.y = value
        elif i == 2:
            self.z = value
        else:
            raise ValueError("id_at index not in range(0, 3)")

    def bisect(self, direction):
        bounds = list(self.bounds)
        key = Key(BoundingBox3D(*bounds))

        key.d = self.d + 1
        key.x = self.x
        key.y = self.y
        key.z = self.z

        def step(i):
            key.set_id_at(i, 2 * key.id_at(i))
            mid = bounds[i] + ((bounds[i + 3] - bounds[i]) / 2.0)
            positive = direction & (1 << i)

            if positive:
                bounds[i] = mid
                key.set_id_at(i, key.id_at(i) + 1)
            else:
                bounds[i + 3] = mid

        for i in range(3):
            step(i)

        key.bounds = BoundingBox3D(*bounds)
        assert key.bounds.width < self.bounds.width
        assert key.bounds.height < self.bounds.height
        assert key.bounds.height - (self.bounds.height / 2.0) < 0.01
        assert key.bounds.width - (self.bounds.width / 2.0) < 0.01
        return key

    def __str__(self):
        return "{}-{}-{}-{}".format(
            self.d, self.x, self.y, self.z
        )

    def __repr__(self):
        return "<Key(d: {}, x: {}, y: {}, z: {})>".format(
            self.d, self.x, self.y, self.z
        )
