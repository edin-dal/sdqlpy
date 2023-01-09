template<typename _Tp, typename K,typename V>
inline void AddMap(_Tp& __x, _Tp& __y) 
{ 
	// X is supposed to be greater than Y
	if (__x.size() < __y.size())
		__x.swap(__y);

	if (__y.size()==0)
		return;

	vector<pair<K,V>> pool;
	const auto& end = __x.end();
	for (auto& p : __y)
	{
		const auto& tmp = __x.find(p.first);
		if (tmp != end)
			tmp->second += p.second;
		else
			pool.emplace_back(p);
	}
	__x.reserve(__x.size() + pool.size());
	__x.insert(pool.begin(), pool.end());
}
