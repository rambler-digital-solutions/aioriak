from riak.mapreduce import RiakMapReduce as _RiakMapReduce, RiakLinkPhase


class RiakMapReduce(_RiakMapReduce):
    def __init__(self, client):
        super().__init__(client)

    async def run(self, timeout=None):
        """
        Submit the map/reduce operation to Riak. Non-blocking wait for result.
        Returns a list of results,
        or a list of links if the last phase is a link phase.

        Example::
                client = await RiakClient.create()
                mr = RiakMapReduce(client)
                mr.add_bucket_key_data('bucket', 'key')
                mr.map(['mr_example', 'get_keys'])
                result = await mr.run()

        :param timeout: Timeout in milliseconds
        :type timeout: integer, None
        :rtype: list
        """
        query, link_results_flag = self._normalize_query()

        result = await self._client.mapred(self._inputs, query, timeout)

        # If the last phase is NOT a link phase, then return the result.
        if not (link_results_flag or
                isinstance(self._phases[-1], RiakLinkPhase)):
            return result

        # If there are no results, then return an empty list.
        if result is None:
            return []

        # Otherwise, if the last phase IS a link phase, then convert the
        # results to link tuples.
        acc = []
        for r in result:
            if len(r) == 2:
                link = (r[0], r[1], None)
            elif len(r) == 3:
                link = (r[0], r[1], r[2])
            else:
                raise ValueError('Invalid format for Link phase result')
            acc.append(link)

        return acc

    async def stream(self, timeout=None):
        """
        Streams the MapReduce query (returns an async iterator).

        Example::
                client = await RiakClient.create()
                mr = RiakMapReduce(client)
                mr.add_bucket_key_data('bucket', 'key')
                mr.map(['mr_example', 'get_keys'])
                async for phase, result in (await mr.stream()):
                    print(phase, result)

        :param timeout: Timeout in milliseconds
        :type timeout: integer
        :rtype: async iterator that yields (phase_num, data) tuples
        """
        query, link_results_flag = self._normalize_query()

        return await self._client.stream_mapred(self._inputs, query, timeout)
