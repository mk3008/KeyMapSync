namespace KeyMapSync;

public interface IResult
{
}

public class Result : IResult
{
    public string? Table { get; set; } = String.Empty;

    public int Count { get; set; } = 0;
}

public class Results : IResult
{
    public string Name { get; set; } = String.Empty;

    public List<IResult> Collection { get; set; } = new();

    internal void Add(IResult result)
        => Collection.Add(result);

    internal void AddRange(List<Result> results)
        => results.ForEach(x => Collection.Add(x));
}