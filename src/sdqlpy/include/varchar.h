template<unsigned maxLen>
struct VarChar{
    wchar_t data[maxLen];

    VarChar()
    {
        for(u_int i=0; i<maxLen; i++)
            data[i] = 0;        
    }

    VarChar(bool nothing)
    {
    }

    VarChar(const VarChar& other)
    {
        for(u_int i=0; i<maxLen; i++)
            data[i] = other.data[i];
    }

    VarChar(const wchar_t* wc)
    {
        for(u_int i=0; i<maxLen; i++)
            data[i] = 0;
        u_int i=0;
        while (wc[i])
        {
            data[i] = wc[i];
            i++;
        }
    }

    bool operator==(const VarChar& other) const 
    {
        for(u_int i=0; i<maxLen; i++)
            if (data[i] != other.data[i])
            return false;
        return true;
    }

    bool operator==(const char* c) const 
    {
        const size_t cSize = strlen(c)+1;
        wchar_t* wc = new wchar_t[cSize];
        mbstowcs (wc, c, cSize);

        if (cSize > maxLen)
            return false;

        for(u_int i=0; i<cSize; i++)
            if (data[i] != wc[i])
                return false;

        for(u_int i=cSize; i<maxLen; i++)
            if (data[i] != 0)
                return false;

        return true;
    }

    bool operator==(const wchar_t* wc) const 
    {
        const size_t cSize = wcslen(wc);

        if (cSize > maxLen)
            return false;

        for(u_int i=0; i<cSize; i++)
            if (data[i] != wc[i])
                return false;

        for(u_int i=cSize; i<maxLen; i++)
            if (data[i] != 0)
                return false;

        return true;
    }

    bool operator!=(const wchar_t* wc) const 
    {
        return !(operator==(wc));
    }

    bool contains(wchar_t* other, int size) const
    {
        if(wcsstr(data, other))
            return true;
        return false;
    }

    int firstIndex(wchar_t* other) const
    {
        auto tmp = wcsstr(data, other);
        if (tmp==nullptr)
            return -1;
        return (tmp - &(data[0]));
    }

    bool startsWith(wchar_t* other) const
    {
        size_t i = 0;
        while (other[i]!= 0)
        {
            if (data[i]==0 || data[i]!=other[i])
                return false;
            i++;
        }
        return true;

        // return (firstIndex(other) == 0);
    }

    bool endsWith(wchar_t* other) const
    {
        size_t data_len = maxLen;
        for(u_int i=0; i<maxLen; i++)
            if (data[i] == 0)
            {
                data_len = i;
                break;
            }
        size_t other_len = wcslen(other);

        return (firstIndex(other) == (data_len-other_len));
    }

    template<unsigned len>
    const VarChar<len> substr(size_t from, size_t to) const
    {
        VarChar<len> res(true);
        size_t j=0;
        for (size_t i = from; i <= to; i++)
            res.data[j++] = data[i];
        return res;
    }

    // const wchar_t* substr(size_t from, size_t to) const
    // {
    //     wchar_t* res = new wchar_t[2+to-from];
    //     res[1+to-from]=0;
    //     size_t j=0;
    //     for (size_t i = from; i <= to; i++)
    //         res[j++]=data[i];
    //     return res;
    // }


};

template <unsigned maxLen>
std::ostream& operator<<(std::ostream& out, const VarChar<maxLen>& value)
{
    for (size_t i = 0; i < maxLen; i++)
        out << (char)value.data[i];
    return out;
}

template <unsigned maxLen>
VarChar<maxLen>& operator+=(VarChar<maxLen>& left, const VarChar<maxLen>& right)
{
    if (left.data[0] == 0)
    {
        left = right;
        return left;
    }
    if (right.data[0] == 0)
    {
        return left;
    }

    std::cout << "Not Allowed VarChar += operation" << std::endl;
}

namespace std
{
   template <unsigned maxLen>
   struct hash<VarChar<maxLen>>
   {
      uint64_t operator()(VarChar<maxLen> const& data) const
      // Hash
      {
         uint64_t result = 0;
         for (unsigned index = 0; index < maxLen; index++)
            result = ((result << 5) | (result >> 27)) ^
                     (static_cast<unsigned char>(data.data[index]));
         return result;
      }
   };
}
