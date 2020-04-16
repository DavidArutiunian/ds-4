namespace TextRankCalc.Models
{
    public class RedisPayloadModel
    {
        public string Description { get; set; }
        public string Data { get; set; }
        public double Rank { get; set; } = 0.0;
    }
}
